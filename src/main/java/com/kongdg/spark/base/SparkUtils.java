package com.kongdg.spark.base;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author userkdg
 * @date 2019/12/8 0:27
 **/
public class SparkUtils extends BaseInit {

    public static final JavaStreamingContext getSsc(SparkSession sparkSession, Duration batchDur) {
        if (batchDur == null) batchDur = Durations.milliseconds(500L);
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), batchDur);
        return new JavaStreamingContext(streamingContext);
    }

    public static SparkSession getSpark(SparkConf conf) {
        return SparkSession.builder().config(conf).getOrCreate();
    }

    public static JavaSparkContext getJsc(SparkConf conf) {
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext getJScBySpark(SparkConf conf, SparkSession spark) {
        return new JavaSparkContext(spark.sparkContext());
    }

    public static JavaSparkContext getJScBySpark(SparkSession spark) {
        return new JavaSparkContext(spark.sparkContext());
    }

}
