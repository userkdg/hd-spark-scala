package com.kongdg.spark.kafka;

import com.google.common.collect.Maps;
import com.kongdg.kafka.MyKafkaProducer;
import com.kongdg.spark.base.SparkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
 *
 * @author userkdg
 * @date 2020-05-27 14:25
 **/
public class ComsumerKafkaSparkStreaming implements Serializable {
    private Map<String, Object> props;

    public static void main(String[] args) {
        ComsumerKafkaSparkStreaming kafkaSparkStreaming = new ComsumerKafkaSparkStreaming();
        kafkaSparkStreaming.initConfig();
        kafkaSparkStreaming.consumer();
    }

    private void consumer() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                // 必须加否则报序列化问题 Serialization stack:
                //        - object not serializable (
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        SparkSession spark = SparkUtils.getSpark(sparkConf);
        JavaStreamingContext ssc = SparkUtils.getSsc(spark, Duration.apply(1000L));
        StreamingContext streamingContext = ssc.ssc();
        streamingContext.checkpoint("F://spark-streaming-checkpoint");
//        DefaultPerPartitionConfig perPartitionConfig = new DefaultPerPartitionConfig(sparkConf);
        // 读取kafka数据
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singleton(MyKafkaProducer.TOPIC_NAME), props));
        directStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
//                System.out.println(consumerRecordJavaRDD);
                if (consumerRecordJavaRDD.isEmpty()) {
                    return;
                }
                consumerRecordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                    @Override
                    public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                        while (consumerRecordIterator.hasNext()) {
                            ConsumerRecord<String, String> consumerRecord = consumerRecordIterator.next();
                            String topic = consumerRecord.topic() + "_of_spark";
                            MyKafkaProducer myKafkaProducer = MyKafkaProducer.getInstance(topic);
                            String value = consumerRecord.value() + "_of_spark";
                            myKafkaProducer.doProducer(value);
                        }
                    }
                });
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                ((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        System.out.println("spark 异步提交kafka offset " + offsets + " 完成");
                    }
                });
            }
        });
        directStream.print();
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Object> properties() {
        Map<String, Object> properties = Maps.newHashMap();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my_groupId");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MyKafkaProducer.KAFKA_HOST);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "clientId");
        return properties;
    }

    private void initConfig() {
        this.props = properties();
    }
}
