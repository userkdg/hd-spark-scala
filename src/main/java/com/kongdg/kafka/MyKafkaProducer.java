package com.kongdg.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Serializable;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author userkdg
 * @date 2020-05-27 13:54
 **/
public class MyKafkaProducer implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String TOPIC_NAME = "test_topic_2";
    public static final String KAFKA_HOST = "192.168.1.5:9092";
    private final List<String> topics;
    private Properties properties;
    private KafkaProducer<String, String> producer;

    public static MyKafkaProducer getInstance(String topic){
        return new MyKafkaProducer(Collections.singletonList(topic));
    }
    public MyKafkaProducer(List<String> topics) {
        this.topics = topics;
        initConfig();
    }

    public static void main(String[] args) {
        MyKafkaProducer myKafkaProducer = new MyKafkaProducer(Collections.singletonList(TOPIC_NAME));
        myKafkaProducer.initConfig();
        myKafkaProducer.doProducer();
    }

    private void initConfig() {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 确保数据完整 ack -1返回
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1000");
        // 即使未达到Batchsize到了时间就会发送日志
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
//        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId"); // 不需要配置，否则会出现InstanceAlreadyExistsException: kafka.producer:type=app-info,id=clientId
        producer = new KafkaProducer<String, String>(properties);
    }

    public void doProducer(String msg) {
        boolean sync = false;
        topics.forEach(topic -> {
            if (sync) {
                try {
                    producer.send(new ProducerRecord<String, String>(topic, msg)).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            } else {
                producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        System.out.println(metadata);
                    }
                });
            }
            producer.flush();
        });
    }

    public void doProducer() {
        int messageNo = 1;
        final int count = Integer.MAX_VALUE;

        while (messageNo < count) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            boolean sync = false;   //是否同步
            topics.forEach(topic -> {
                if (sync) {
                    try {
                        producer.send(new ProducerRecord<String, String>(topic, data)).get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                } else {
                    producer.send(new ProducerRecord<String, String>(topic, data));
                }
            });

            //必须写下面这句,相当于发送
            producer.flush();

            messageNo++;
        }
    }
}
