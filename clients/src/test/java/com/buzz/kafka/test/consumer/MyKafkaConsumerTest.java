package com.buzz.kafka.test.consumer;

import com.buzz.kafka.test.Env;
import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author bairen
 * @description
 **/
public class MyKafkaConsumerTest {


    private void processMessage(ConsumerRecords<String, byte[]> records, String topic) {
        SourceLogger.info("receive topic:[" + topic + "] records size:[" + records.count()+"]");
        if (records.isEmpty()) {
            return;
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.records(topic).iterator();
        while (iterator.hasNext()) {
            // maxPollRecords
            ConsumerRecord<String, byte[]> record = iterator.next();
            long latency = System.currentTimeMillis() - record.timestamp();
            SourceLogger.info(record.topic() + "\t partition:"
                    + record.partition() + "\t offset:" + record.offset());

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 测试 auto.commit
     */
    @Test
    public void testAutoCommit() throws InterruptedException {
        String topic = "manyou.test";
        KafkaConsumer<String, byte[]> consumer = KafkaClientBuilder.builder()
                .env(Env.TEST)
                .groupId("kafka.test")
                .topic(topic)
                .maxPoll(3)
                .build();
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            SourceLogger.info("send consumer.poll()");
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            if (records.count() == 0) {
                continue;
            }
            processMessage(records, topic);
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
                .build();
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            //Thread.sleep(1000);
            if (records.count() == 0) {
                continue;
            }
            processMessage(records, topic);
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
                .build();

        topic = "manyou.test";
        consumer.assign(Collections.singleton(new TopicPartition(topic, 0)));
        consumer.seek(new TopicPartition(topic, 0), 1847237);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
            //SourceLogger.info(this.getClass(), "============================");
            if (records.count() == 0) {
                continue;
            }
            processMessage(records, topic);
            break;
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
                .build();
        TopicPartition tp = new TopicPartition(topic, 0);
        long end = consumer.beginningOffsets(Collections.singletonList(tp)).get(tp);
        System.out.println("end=" + end);
    }
}
