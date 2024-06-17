package com.buzz.kafka.test.producer;

import com.buzz.kafka.test.Env;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Author: bairen
 * Date:2024/4/25 11:03
 */
public class KafkaProducerBuilder {

    private Env env = Env.TEST;

    public static KafkaProducerBuilder builder() {
        return new KafkaProducerBuilder();
    }

    public KafkaProducerBuilder env(Env env) {
        this.env = env;
        return this;
    }

    public KafkaProducer<String, byte[]> buildKafkaProducer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", env.getServers());
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("acks", "-1");
        props.setProperty("retries", "0");
        props.setProperty("linger.ms", "100");
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        return producer;
    }
}
