package com.buzz.kafka.network;

import com.buzz.kafka.test.Env;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

public class FetcherTest {

    /**
     * 查询消费者CommitOffset
     */
    @Test
    public void testCommitOffset() {
        OffsetFetcher fetcher = new OffsetFetcher("bairen.test.1", Env.TEST);
        fetcher.subscribe(Collections.singleton(new TopicPartition("manyou.test", 3)));
        fetcher.poll(Duration.ofSeconds(600));
    }

    @Test
    public void testRequestFuture() {
        RequestFuture future = new RequestFuture();
        System.out.println(future.isDone());
    }

    @Test
    public void testFetchRecord() {
        RecordsFetcher recordsFetcher = new RecordsFetcher(Env.TEST);
        recordsFetcher.assign(new TopicPartition("manyou.test", 0), 1676915);
        recordsFetcher.poll(Duration.ofSeconds(600));
    }
}
