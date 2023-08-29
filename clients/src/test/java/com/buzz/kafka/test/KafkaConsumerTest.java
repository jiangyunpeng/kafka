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
        System.out.println("============================");
        System.out.println("poll " + topic + " records size:" + records.count());
        if (records.isEmpty()) {
            return;
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.records(topic).iterator();
        while (iterator.hasNext()) {
            // maxPollRecords
            ConsumerRecord<String, byte[]> record = iterator.next();
            long latency = System.currentTimeMillis() - record.timestamp();
            System.out.println(record.topic() + "\t partition:"
                    + record.partition() + "\t offset:" + record.offset() + "\t latency: " + latency + "ms");
        }
        System.exit(-1);
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
                .maxPoll(100)
                .buildKafkaConsumer();
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            SourceLogger.info(this.getClass(), "============================");
            if (records.count() == 0) {
                continue;
            }
            printResult(records, topic);
            break;
        }
    }

    /**
     * @throws InterruptedException
     */
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
            //Thread.sleep(1000);
            if (records.count() == 0) {
                continue;
            }
            printResult(records, topic);
        }
    }

    @Test
    public void testAssign() throws InterruptedException {
        String topic = null;
        KafkaConsumer<String, byte[]> consumer = KafkaClientBuilder.builder()
                .env(Env.TEST)
                .topic(topic)
                .partition(0)
                .maxPoll(5)
                .buildKafkaConsumer();

        topic = "manyou.test";
        consumer.assign(Collections.singleton(new TopicPartition(topic, 0)));
        consumer.seek(new TopicPartition(topic, 0), 1847237);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            SourceLogger.info(this.getClass(), "============================");
            if (records.count() == 0) {
                continue;
            }
            printResult(records, topic);
            break;
        }


    }

}
