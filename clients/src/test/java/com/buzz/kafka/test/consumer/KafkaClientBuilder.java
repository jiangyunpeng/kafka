package com.buzz.kafka.test.consumer;

import com.buzz.kafka.test.Env;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    private boolean autoCommit = true;
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
    public KafkaConsumer<String, byte[]> build() {
        return new KafkaConsumer<String, byte[]>(getConsumerProperties());
    }
    private Properties getConsumerProperties() {
        Properties props = new Properties();
        props.putAll(getDefaultProperties());

        if(autoCommit){
            props.setProperty("enable.auto.commit", "true");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //latest或者earliest
        }else if(offset>=0){ //seek模式
            //group.id 参数不设置这样的consumer就是纯proxy模式
            props.setProperty("enable.auto.commit", "false");
            //kafka不会自动管理offset，同时enable.auto.commit必须设置为false
            props.put("auto.offset.reset", "none");//offset范围超限报错
        }else{
            props.setProperty("enable.auto.commit", "false");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //latest或者earliest
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
        return props;
    }

}