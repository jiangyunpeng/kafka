package com.buzz.kafka.test.consumer;

import com.buzz.kafka.test.Env;
import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Author: bairen
 * Date:2024/4/23 10:30
 */
public class BuzzConsumer implements Runnable {

    private Collection<String> topics;
    private String group;
    private AtomicBoolean running = new AtomicBoolean(false);
    private KafkaConsumer<String, byte[]> consumer;

    public BuzzConsumer(String group) {
        this(null, group);
    }

    public BuzzConsumer(String topic, String group) {
        this.group = group;
        initKafkaConsumer();
        if (topic != null) {
            this.subscribe(topic);
        }
    }

    private void initKafkaConsumer() {
        consumer = KafkaClientBuilder.builder()
                .env(Env.TEST)
                .groupId(group)
                .maxPoll(5)
                .autoCommit(false)
                .build();
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            Thread thread = new Thread(this, "buzz-kafka-consumer");
            thread.start();
        }
    }

    public void await() {
        while (running.get()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
    }

    public void stop() {
        running.compareAndSet(true, false);
    }


    @Override
    public void run() {
        while (running.get()) {
            try {
                //SourceLogger.info("consumer.poll()");
                ConsumerRecords<String, byte[]> records = poll();
                if (records.count() == 0) {
                    continue;
                }
                processMessage(records);
            } catch (Throwable e) {
               SourceLogger.error(this.getClass(),"poll error",e);
                stop();
            }

        }
    }

    protected synchronized ConsumerRecords<String, byte[]> poll() {
        return consumer.poll(Duration.ofMillis(200));
    }

    public synchronized void pause() {
        SourceLogger.info("pause partition {}", consumer.assignment());
        consumer.pause(consumer.assignment());
    }

    public synchronized void assign(Collection<TopicPartition> partitions){
        consumer.assign(partitions);
    }

    /**
     * 获取最新的offset
     *
     * @param partitions
     * @return
     */
    public synchronized Long endOffsets(TopicPartition partitions) {
        return consumer.endOffsets(Collections.singleton(partitions)).get(partitions);
    }

    /**
     * 最老的offset
     *
     * @param partitions
     * @return
     */
    public synchronized Long beginningOffsets(TopicPartition partitions) {
        return consumer.beginningOffsets(Collections.singleton(partitions)).get(partitions);
    }

    /**
     * @param partition
     * @param offset
     */
    public synchronized void seek(TopicPartition partition, long offset) {
        SourceLogger.info("seek partition:{} to offset:{}", partition, offset);
        consumer.seek(partition, offset);
    }

    public synchronized void subscribe(String topics) {
        SourceLogger.info("subscribe topic {}", topics);
        if (topics == null) {
            consumer.subscribe(Collections.emptyList());
        } else {
            consumer.subscribe(Arrays.asList(topics.split(",")));
        }
        this.topics = Arrays.asList(topics.split(","));
    }

    public synchronized void subscribe(String topics, ConsumerRebalanceListener listener) {
        SourceLogger.info("subscribe topic {}", topics);
        consumer.subscribe(Arrays.asList(topics.split(",")), listener);
    }

    /**
     * 线程安全,commit时不允许poll
     */
    protected synchronized void commit() {
        consumer.commitAsync();
    }

    protected void processMessage(ConsumerRecords<String, byte[]> records) {
        SourceLogger.info("receive records size:[" + records.count() + "]");
        if (records.isEmpty()) {
            return;
        }
        Iterator<ConsumerRecord<String, byte[]>> it = records.iterator();
        while (it.hasNext()) {
            ConsumerRecord<String, byte[]> record = it.next();
            long latency = System.currentTimeMillis() - record.timestamp();
            SourceLogger.info(record.topic() + "\t partition:"
                    + record.partition() + "\t offset:" + record.offset());

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        commit();
        onProcessedMessage();
    }


    protected void onProcessedMessage() {

    }
}
