package com.kongdg.spark.streaming;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.ui.StreamingJobProgressListener;
import scala.Tuple2;

/**
 * @author userkdg
 * @date 2020/3/17 22:25
 **/
public class CalcHdfsFileOnSparkStreaming {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        SparkContext sc = spark.sparkContext();
//        Checkpoint.checkpointFile("hdfs:ns1:9000/user/spark/checkpoint", Time.apply(TimeUnit.SECONDS.toMillis(1)));
        StreamingContext streamingContext = new StreamingContext(sc, Durations.milliseconds(10));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
        javaStreamingContext.addStreamingListener(new StreamingJobProgressListener(streamingContext));
        javaStreamingContext.checkpoint("hdfs://192.168.1.4:8020/spark/checkpoint");
        JavaPairInputDStream<Void, SimpleGroup> javaPairInputDStream = javaStreamingContext
                .fileStream("hdfs://192.168.1.4:9000/spark/test_parquet"
                , Void.class
                , SimpleGroup.class
                , ParquetInputFormat.class);
        javaPairInputDStream.mapToPair(new PairFunction<Tuple2<Void, SimpleGroup>, String, Object>() {
            @Override
            public Tuple2<String, Object> call(Tuple2<Void, SimpleGroup> voidSimpleGroupTuple2) throws Exception {
                
                return null;
            }
        });
        JavaDStream<Tuple2<Void, SimpleGroup>> window = javaPairInputDStream.toJavaDStream().window(Durations.minutes(20), Durations.minutes(10));


        try {
            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
