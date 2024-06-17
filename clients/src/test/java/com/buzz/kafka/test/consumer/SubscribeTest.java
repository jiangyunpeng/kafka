package com.buzz.kafka.test.consumer;

import com.buzz.kafka.test.CountDown;
import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 订阅
 * <p>
 * Author: bairen
 * Date:2024/4/23 10:28
 */
public class SubscribeTest {


    /**
     * 测试修改订阅
     *
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testAdjustSubScribe() throws InterruptedException, IOException {
        CountDown countDown = new CountDown(1);
        BuzzConsumer consumer = new BuzzConsumer("manyou.test,bairen.test.2000", "kafka.test") {
            public void onProcessedMessage() {
                countDown.countDown();
            }
        };
        consumer.start();
        countDown.await();

        consumer.subscribe("bairen.test");
        countDown.reset();
        countDown.await(3000,TimeUnit.MILLISECONDS);
    }

    /**
     * 测试消费暂停
     */
    @Test
    public void testPause() throws InterruptedException {
        CountDown countDown = new CountDown(1);
        BuzzConsumer consumer = new BuzzConsumer("manyou.test,bairen.test.2000", "kafka.test") {
            public void onProcessedMessage() {
                countDown.countDown();
            }
        };
        consumer.start();
        countDown.await();
        consumer.pause();
        countDown.reset();
        countDown.await(3000,TimeUnit.MILLISECONDS);
        SourceLogger.info("end");
    }


    /**
     * 测试 seek
     * @throws InterruptedException
     */
    @Test
    public void testSeek() throws InterruptedException {
        CountDown countDown = new CountDown(6);
        String topic = "manyou.test";
        BuzzConsumer consumer = new BuzzConsumer(null, "manyou.test.group") {
            public void onProcessedMessage() {
                countDown.countDown();
            }
        };

        List<TopicPartition> partitions = IntStream.range(0, 10)
                .mapToObj(i -> new TopicPartition(topic, i))
                .collect(Collectors.toList());
        consumer.assign(partitions);

        for(int i=0;i<6;++i){
            TopicPartition partition = new TopicPartition(topic,i);
            long offset = consumer.endOffsets(partition);
            consumer.seek(partition,offset-1);
        }
        consumer.start();
        countDown.await();

    }

    @Test
    public void testRebalance() throws InterruptedException {
        CountDown countDown = new CountDown(Integer.MAX_VALUE);
        String topic = "manyou.test";
        BuzzConsumer consumer = new BuzzConsumer("manyou.test.group") {
            public void onProcessedMessage() {
                countDown.countDown();
            }
        };
        consumer.subscribe(topic, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                SourceLogger.info("onPartitionsRevoked {}",partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                SourceLogger.info("onPartitionsAssigned {}",partitions);
            }
        });
        consumer.start();
        countDown.await();
    }

}
