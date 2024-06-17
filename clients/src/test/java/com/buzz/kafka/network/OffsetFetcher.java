package com.buzz.kafka.network;

import com.buzz.kafka.test.Env;
import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.clients.consumer.internals.RequestFutureAdapter;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * MyFetcher 获取CommitOffset
 * <p>
 * 参考： ConsumerCoordinator.fetchCommittedOffsets()
 * 注意: 在查询consumer信息，必须通过对应的coordinator节点
 *
 *
 * <pre>
 *                    +---------------------------------------------------+
 *                    v                                                   |
 * +-----------+     +-----------------------+     +---------------+     +--------------------------+
 * | OffsetFetcher | --> | ConsumerNetworkClient | --> | NetworkClient | --> | RequestCompletionHandler |
 * +-----------+     +-----------------------+     +---------------+     +--------------------------+
 *                                                   |
 *                                                   |
 *                                                   v
 *                                                 +---------------+
 *                                                 |   Selector    |
 *                                                 +---------------+
 * </pre>
 * 两阶段：
 * 阶段1 准备发送：
 * 1. OffsetFetcher 调用 ConsumerNetworkClient.send()创建请求，得到RequestFuture
 * 2. ConsumerNetworkClient 调用 NetworkClient 创建 request 并关联RequestCompletionHandler，返回RequestFuture，同时把request保存在本地
 * 阶段2 发送并接收数据：
 * 1. OffsetFetcher 调用 ConsumerNetworkClient.poll()
 * 2. ConsumerNetworkClient 从本地队列中取出request调用NetworkClient.send()发送
 * 3. ConsumerNetworkClient 调用NetworkClient.poll()等待服务端响应
 * 4. NetworkClient收到服务端响应响应，回调RequestCompletionHandler
 * 5. RequestCompletionHandler 会回调ConsumerNetworkClient中创建的future
 */
public class OffsetFetcher {
    private Time time = new SystemTime();
    private String groupId;
    private Node coordinator;
    private ConsumerNetworkClient client;
    private long retryBackoffMs = 100; //config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> pendingRequestFuture;
    private RequestFuture<Void> findCoordinatorFuture;

    private Set<TopicPartition> partitions;

    public OffsetFetcher(String groupId, Env env) {
        this.groupId = groupId;
        NetClientBuilder builder = NetClientBuilder.builder().build(groupId, env);
        client = new ConsumerNetworkClient(builder.getNetClient(), builder.getMetadata());
    }

    public void subscribe(Set<TopicPartition> partitions) {
        this.partitions = partitions;
    }

    /**
     * 查询 commit offset
     *
     * @param timeout
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> poll(Duration timeout) {
        Timer timer = time.timer(timeout);
        do {
            if (!ensureCoordinator(timer)) {
                return Collections.emptyMap();
            }
            System.out.println("coordinator ready ");
            Map<TopicPartition, OffsetAndMetadata> ret = this.fetchCommitOffset(partitions, timer);
            System.out.println("ret " + ret);
            return ret;
        } while (timer.notExpired());
    }

    //参考 AbstractCoordinator.lookupCoordinator()
    private boolean ensureCoordinator(Timer timer) {
        do {
            if (coordinatorReady()) {
                return true;
            }
            RequestFuture future = sendFindCoordinatorRequest();
            client.poll(future, timer);

            //如果超时直接break
            if (!future.isDone()) {
                break;
            }
            if (future.failed()) {
                throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                this.coordinator = null;
                timer.sleep(retryBackoffMs);
            }
        } while (timer.notExpired());

        return coordinatorReady();
    }

    private boolean coordinatorReady() {
        boolean ret = coordinator != null && !client.isUnavailable(coordinator);
        if (!ret) {
            coordinator = null;
        }
        return ret;
    }

    protected synchronized RequestFuture<Void> sendFindCoordinatorRequest() {
        if (findCoordinatorFuture == null) {
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                throw new RuntimeException("no node!");
            }

            FindCoordinatorRequest.Builder requestBuilder =
                    new FindCoordinatorRequest.Builder(
                            new FindCoordinatorRequestData()
                                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                                    .setKey(this.groupId));

            findCoordinatorFuture = client.send(node, requestBuilder)
                    .compose(new FindCoordinatorResponseHandler());

        }
        return findCoordinatorFuture;
    }


    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                                .setKey(this.groupId));
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler());
    }


    public Map<TopicPartition, OffsetAndMetadata> fetchCommitOffset(Set<TopicPartition> partitions, Timer timer) {
        do {
            try {
                if (pendingRequestFuture == null) {
                    pendingRequestFuture = sendOffsetRequest(partitions);
                }
                client.poll(pendingRequestFuture, timer);
                if (pendingRequestFuture.isDone()) {
                    if (pendingRequestFuture.succeeded()) {
                        return pendingRequestFuture.value();
                    } else if (!pendingRequestFuture.isRetriable()) {
                        throw pendingRequestFuture.exception();
                    }
                }
                timer.sleep(retryBackoffMs);
            } finally {
                pendingRequestFuture = null;
            }
        } while (timer.notExpired());
        return null;
    }

    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetRequest(Set<TopicPartition> partitions) {
        OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.groupId,
                true,
                new ArrayList<TopicPartition>(partitions),
                true
        );

        //并没有真正发送请求，只是暂存，同时注册了handler
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }


    private class OffsetFetchResponseHandler extends RequestFutureAdapter<ClientResponse, Map<TopicPartition, OffsetAndMetadata>> {

        @Override
        public void onSuccess(ClientResponse value, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {

            OffsetFetchResponse response = (OffsetFetchResponse) value.responseBody();

            if (response.hasError()) {
                Errors error = response.error();
                future.raise(error);
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Entry<TopicPartition, PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    System.out.println("has error");
                    System.exit(-1);
                    return;
                }

                if (data.offset >= 0) {
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.leaderEpoch, data.metadata));
                }
            }
            future.complete(offsets);
        }
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            //clear future
            findCoordinatorFuture = null;

            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            Errors error = findCoordinatorResponse.error();
            if (error == Errors.NONE) {
                int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.node().id();
                OffsetFetcher.this.coordinator = new Node(
                        coordinatorConnectionId,
                        findCoordinatorResponse.node().host(),
                        findCoordinatorResponse.node().port());
                SourceLogger.info(this.getClass(), "find coordinator {}", coordinator);
                client.tryConnect(coordinator);
                future.complete(null);

            } else {
                future.raise(error);
            }

        }
    }
}
