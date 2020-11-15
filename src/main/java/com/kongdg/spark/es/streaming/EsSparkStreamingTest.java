package com.kongdg.spark.es.streaming;

import com.google.common.collect.ImmutableList;
import com.kongdg.spark.base.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.streaming.api.java.JavaEsSparkStreaming;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author userkdg
 * @date 2020-04-06 14:46
 **/
public class EsSparkStreamingTest {
    @Test
    public void saveToEs(){
        String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
        String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";

        try (SparkSession spark = SparkUtils.getSpark(new SparkConf().setMaster("local[*]"))) {
            try (JavaSparkContext jsc = SparkUtils.getJScBySpark(spark)) {
                try (JavaStreamingContext jssc = SparkUtils.getSsc(spark, Durations.milliseconds(500L))) {
                    JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2));
                    Queue<JavaRDD<String>> microbatches = new LinkedList<JavaRDD<String>>();
                    microbatches.add(stringRDD);
                    JavaDStream<String> stringDStream = jssc.queueStream(microbatches);
                    JavaEsSparkStreaming.saveJsonToEs(stringDStream, "spark/docs");
                    jssc.start();
                }
            }
        }


    }
}
