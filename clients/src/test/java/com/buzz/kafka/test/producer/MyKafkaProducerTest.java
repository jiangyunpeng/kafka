package com.buzz.kafka.test.producer;

import org.apache.kafka.SourceLogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Author: bairen
 * Date:2024/4/25 11:01
 */

public class MyKafkaProducerTest {


    private void doSend(KafkaProducer<String, byte[]> producer) throws ExecutionException, InterruptedException {
        byte[] data = "this is a test message".getBytes();

        for(int i=0;i<6;++i){
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("manyou.test",
                    i,
                    null,
                    data);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        SourceLogger.info("send success partition:[{}], offset:[{}]", recordMetadata.partition(),recordMetadata.offset());
                    } else {
                        SourceLogger.info("send failed", e);
                    }
                }
            }).get();
        }
    }
    @Test
    public void test() throws ExecutionException, InterruptedException {
        KafkaProducer<String, byte[]> producer = KafkaProducerBuilder.builder().buildKafkaProducer();
        for(int i=0;i<3;++i){
            doSend(producer);
        }

    }
}
