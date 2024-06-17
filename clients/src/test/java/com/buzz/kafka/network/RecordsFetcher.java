package com.buzz.kafka.network;

import com.buzz.kafka.test.Env;
import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 手动拉取消息
 */
public class RecordsFetcher {

    private int maxWaitMs = 500;
    private int minBytes = 1;
    private int maxBytes = 1048576; //1mb
    private int fetchSize = 10;
    private long retryBackoffMs = 100; //retry.backoff.ms
    private final Time time;
    private final SubscriptionState subscriptions;
    private final Metadata metadata;
    private final ConsumerNetworkClient client;

    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches = new ConcurrentLinkedQueue<>();
    private final Map<Integer, FetchSessionHandler> sessionHandlers = new ConcurrentHashMap<>();

    public RecordsFetcher( Env env) {
        this.time = new SystemTime();
        LogContext logContext = new LogContext("[Consumer clientId=myTest] ");
        this.subscriptions = new SubscriptionState(logContext,OffsetResetStrategy.NONE);
        NetClientBuilder builder = NetClientBuilder.builder().build("dummy", env);
        this.metadata = builder.getMetadata();
        this.client = new ConsumerNetworkClient(builder.getNetClient(),metadata);
    }

    public void assign(TopicPartition topicPartition, long offset) {
        //手动分配
        subscriptions.assignFromUser(Collections.singleton(topicPartition));
        //设置点位
        subscriptions.seek(topicPartition, offset);
        //设置metadata，发送metadata请求需要
        //metadata.setTopics(Collections.singleton(topicPartition.topic()));
    }


    public void poll(Duration timeout) {
        Timer timer = time.timer(timeout);
        do {
            if(!maybeUpdateMetadata(timer)){
                continue;
            }
            sendFetches();
            Timer pollTimer = time.timer(retryBackoffMs);
            client.poll(pollTimer, () -> {
                return !completedFetches.isEmpty();
            });
            pollForFetches();

        } while (timer.notExpired());
    }

    private boolean maybeUpdateMetadata(Timer timer){
        //如果需要更新
        if(this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0){
            return client.awaitMetadataUpdate(timer);
        }
        return true;
    }
    private void pollForFetches() {
        CompletedFetch completedFetch = completedFetches.poll();
        if(completedFetch==null){
            return;
        }
        TopicPartition tp = completedFetch.partition;
        FetchResponse.PartitionData<Records> partition = completedFetch.partitionData;
        Iterator<? extends RecordBatch> batches = partition.records().batches().iterator();
        while(batches.hasNext()){
            RecordBatch recordBatch =  batches.next();
            Iterator<Record> it = recordBatch.iterator();
            while(it.hasNext()){
                Record record = it.next();
                System.out.println("receive "+record.toString());
            }
        }

        System.exit(-1);
    }

    Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs) {
        Optional<Integer> nodeId = subscriptions.preferredReadReplica(partition, currentTimeMs);
        if (nodeId.isPresent()) {
            Optional<Node> node = nodeId.flatMap(id -> metadata.fetch().nodeIfOnline(partition, id));
            if (node.isPresent()) {
                return node.get();
            } else {
                subscriptions.clearPreferredReadReplica(partition);
                return leaderReplica;
            }
        } else {
            return leaderReplica;
        }
    }

    private void sendFetches() {
        Set<TopicPartition> exclude = new HashSet<>();
        for (TopicPartition partition : subscriptions.fetchablePartitions(tp -> !exclude.contains(tp))) {

            SubscriptionState.FetchPosition position = this.subscriptions.position(partition);
            if (position == null) {
                throw new IllegalStateException("Missing position for fetchable partition " + partition);
            }
            Optional<Node> leaderOpt = position.currentLeader.leader;
            if (!leaderOpt.isPresent()) {
                SourceLogger.info("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate();
                continue;
            }
            long currentTimeMs = System.currentTimeMillis();
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs); //选择node
            if (node == null) {
                metadata.requestUpdate();
            } else if (client.isUnavailable(node)) {
                //SourceLogger.info(this.getClass(), "node {} not ready ", node);
            } else if (client.isPendingRequest(node)) {
                //SourceLogger.info(this.getClass(), "node {} hasInFlightRequests ", node);
            } else {

                FetchSessionHandler sessionHandler = sessionHandlers.computeIfAbsent(node.id(), (k) -> {
                    return new FetchSessionHandler(new LogContext(), node.id());
                });

                FetchSessionHandler.Builder builder = sessionHandler.newBuilder();
                builder.add(partition, new FetchRequest.PartitionData(position.offset, FetchRequest.INVALID_LOG_START_OFFSET,
                        this.fetchSize, Optional.empty()));
                FetchRequestData requestData = builder.build();

                final FetchRequest.Builder request = FetchRequest.Builder
                        .forConsumer(this.maxWaitMs, this.minBytes, requestData.toSend()) //最大等待时间和最小bytes
                        .isolationLevel(IsolationLevel.READ_UNCOMMITTED)
                        .setMaxBytes(this.maxBytes)
                        .metadata(requestData.metadata())
                        .toForget(requestData.toForget());

                client.send(node, request).addListener(new RequestFutureListener<ClientResponse>() {

                    @Override
                    @SuppressWarnings("unchecked")
                    public void onSuccess(ClientResponse value) {
                        FetchResponse<Records> response = (FetchResponse<Records>) value.responseBody();

                        FetchSessionHandler handler = sessionHandlers.get(node.id());
                        if (handler == null) {
                            throw new IllegalStateException("handler is null");
                        }
                        if (!handler.handleResponse(response)) {
                            return;
                        }

                        SourceLogger.info(RequestFutureListener.class, "收到 FETCH 响应 data {}", response.responseData());
                        for (Entry<TopicPartition, PartitionData<Records>> entry : response.responseData().entrySet()) {
                            TopicPartition topicPartition = entry.getKey();
                            PartitionData<Records> fetchData = entry.getValue();
                            long fetchedOffset = requestData.sessionPartitions().get(partition).fetchOffset;
                            completedFetches.add(new CompletedFetch(topicPartition, fetchedOffset, fetchData, value.requestHeader().apiVersion()));

                        }
                    }

                    @Override
                    public void onFailure(RuntimeException e) {

                    }
                });
            }
        }
    }

    private static class CompletedFetch {
        private final TopicPartition partition;
        private final long fetchedOffset;
        private final FetchResponse.PartitionData<Records> partitionData;
        private final short responseVersion;

        public CompletedFetch(TopicPartition partition, long fetchedOffset, PartitionData<Records> partitionData, short responseVersion) {
            this.partition = partition;
            this.fetchedOffset = fetchedOffset;
            this.partitionData = partitionData;
            this.responseVersion = responseVersion;
        }

        public TopicPartition getPartition() {
            return partition;
        }

        public long getFetchedOffset() {
            return fetchedOffset;
        }

        public PartitionData<Records> getPartitionData() {
            return partitionData;
        }

        public short getResponseVersion() {
            return responseVersion;
        }
    }

}
