package com.buzz.kafka.test;

import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author bairen
 * @description
 **/
public class KafkaConsumerTest {


    private void printResult(ConsumerRecords<String, byte[]> records, String topic) {
        SourceLogger.info("poll records size:" + records.count());
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.records(topic).iterator();
        while (iterator.hasNext()) {
            // maxPollRecords
            ConsumerRecord<String, byte[]> record = iterator.next();
            long latency = System.currentTimeMillis() - record.timestamp();
            System.out.println(record.topic() + "\t partition:"
                    + record.partition() + "\t offset:" + record.offset() + "\t latency: " + latency + "ms");
        }
    }


    /**
     * 查询topic offset区间
     */
    @Test
    public void testGetEndOffset() {
        String topic = "manyou.test";
        KafkaConsumer<String, byte[]> consumer = KafkaClientBuilder.builder()
                .offset(0)
                .env(Env.TEST)
                .buildKafkaConsumer();
        TopicPartition tp = new TopicPartition(topic, 0);
        long end = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
        System.out.println("end=" + end);
    }


    /**
     * 测试 subscribe 模式
     */
    @Test
    public void testSubscribe1() throws InterruptedException {
        String topic = "manyou.test";
        KafkaConsumer<String, byte[]> consumer = KafkaClientBuilder.builder()
                .env(Env.TEST)
                .groupId("bairen.test.1")
                .partition(0)
                .topic(topic)
                .maxPoll(5)
                .buildKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
            SourceLogger.info(this.getClass(),"============================");
            Thread.sleep(5000);
            if (records.count() == 0) {
                continue;
            }
            printResult(records, topic);
        }
    }

    @Test
    public void testSubscribe2() throws InterruptedException {
        String topic = "transsonic-test";
        KafkaConsumer<String, byte[]> consumer = KafkaClientBuilder.builder()
                .env(Env.TEST)
                .groupId("bairen.test.1")
                .partition(0)
                .topic(topic)
                .maxPoll(5)
                .buildKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            SourceLogger.info(this.getClass(),"============================");
            Thread.sleep(1000);
            if (records.count() == 0) {
                continue;
            }
            printResult(records, topic);
        }
    }

}
