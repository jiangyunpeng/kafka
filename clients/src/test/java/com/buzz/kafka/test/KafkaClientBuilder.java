package com.buzz.kafka.test;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author bairen
 * @description
 **/
public class KafkaClientBuilder {
    private static AtomicInteger id = new AtomicInteger(1);

    public static KafkaClientBuilder builder() {
        return new KafkaClientBuilder();
    }

    private String topic;
    private int partition = 0;
    private int offset = -1;
    private Env env = Env.TEST;
    private boolean autoCommit = false;
    private String groupId = "zion-neo";
    private int maxPoll = 10;

    public KafkaClientBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaClientBuilder partition(int partition) {
        this.partition = partition;
        return this;
    }

    public KafkaClientBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    public KafkaClientBuilder env(Env env) {
        this.env = env;
        return this;
    }

    public KafkaClientBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public KafkaClientBuilder autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public KafkaClientBuilder maxPoll(int maxPoll) {
        this.maxPoll = maxPoll;
        return this;
    }


    //============================ build ==========================================

    /**
     * build KafkaConsumer
     *
     * @return
     */
    public KafkaConsumer<String, byte[]> buildKafkaConsumer() {
        return new KafkaConsumer<String, byte[]>(getConsumerProperties());
    }
    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.putAll(getDefaultProperties());
        //props.put("max.partition.fetch.bytes",1024*100);
        //props.put("fetch.max.wait.ms",200);
        if (offset < 0) {
            //sub模式，业务客户端
            props.setProperty("group.id", groupId);
            props.setProperty("enable.auto.commit", "true");
            props.setProperty("auto.offset.reset", "earliest"); //无提交的offset时，从头开始消费
        } else {
            //seek模式，类似代理
            //group.id 参数不设置这样的consumer就是纯proxy模式
            props.setProperty("enable.auto.commit", "false");
            //kafka不会自动管理offset，同时enable.auto.commit必须设置为false
            props.put("auto.offset.reset", "none");//offset范围超限报错
        }
        return props;
    }

    public Properties getProduceProperties() {
        Properties props = new Properties();
        props.putAll(getDefaultProperties());
        props.put("max.block.ms", 10 * 1000);
        props.put("acks", "1");
        //还有一些配置是可选的
//      linger.ms，这个是批量发送的等待时间，如果设置为500毫秒，表示等待500ms这一批数据将会被提交
//		props.put("linger.ms", 500);
//		props.put("request.timeout.ms", 500);
//		props.put("delivery.timeout.ms", 1000);
        return props;
    }

    public Properties getDefaultProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", env.getServers());
        props.put("client.id",  "client-"+id.getAndIncrement());
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);
        props.put("max.poll.records", String.valueOf(maxPoll));//单次消费者拉取的最大数据条数
        props.put("enable.auto.commit", "false");
        return props;
    }

}