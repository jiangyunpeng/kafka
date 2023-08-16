package com.buzz.kafka.network;

import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient.PollCondition;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.Timer;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class ConsumerNetworkClient {

    private NetworkClient netClient;

    private ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent = new ConcurrentHashMap<>();

    public ConsumerNetworkClient(NetworkClient networkClient) {
        this.netClient = networkClient;
    }

    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        long now = System.currentTimeMillis();

        //通过 completionHandler 关联future
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();

        //创建请求
        ClientRequest clientRequest = netClient.newClientRequest(node.idString(), requestBuilder, now, true,
                3000, completionHandler);

        //添加到队列中
        ConcurrentLinkedQueue queue = unsent.computeIfAbsent(node, (n) -> new ConcurrentLinkedQueue<>());
        queue.add(clientRequest);
        netClient.wakeup();

        //返回
        return completionHandler.future;
    }

    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    public void poll(Timer timer, PollCondition condition) {
        long pollDelayMs = trySend(timer.currentTimeMs());
        if (condition.shouldBlock()) {
            long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
            netClient.poll(pollTimeout, timer.currentTimeMs());
        }
        timer.update();
        // try again to send requests since buffer space may have been
        // cleared or a connect finished in the poll
        trySend(timer.currentTimeMs());
        clean();
    }

    private long trySend(long now) {
        for (Node node : unsent.keySet()) {
            Iterator<ClientRequest> iterator = requestIterator(node);
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                if (netClient.ready(node, now)) {
                    SourceLogger.info(this.getClass(), "send_request {} to {}", request.requestBuilder().apiKey(), node.idString());
                    netClient.send(request, now);
                    iterator.remove();
                } else {
                    SourceLogger.info(this.getClass(), "wait_send {} client {} not ready", request.requestBuilder().apiKey(), node.idString());
                }
            }
        }
        return 5000;
    }


    public Node leastLoadedNode() {
        return netClient.leastLoadedNode(System.currentTimeMillis());
    }

    public boolean isUnavailable(Node node) {
        return netClient.connectionFailed(node) && netClient.connectionDelay(node, System.currentTimeMillis()) > 0;
    }

    public boolean isPendingRequest(Node node){
        return netClient.hasInFlightRequests(node.idString());
    }

    public void tryConnect(Node node) {
        netClient.ready(node, System.currentTimeMillis());
    }

    public Iterator<ClientRequest> requestIterator(Node node) {
        ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
        return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
    }

    public void clean() {
        // the lock protects removal from a concurrent put which could otherwise mutate the
        // queue after it has been removed from the map
        synchronized (unsent) {
            Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
            while (iterator.hasNext()) {
                ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                if (requests.isEmpty())
                    iterator.remove();
            }
        }
    }

    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;

        public RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        @Override
        public void onComplete(ClientResponse response) {
            future.complete(response);
        }
    }
}
