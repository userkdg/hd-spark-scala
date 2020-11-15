package com.kongdg.spark.streaming;

import com.kongdg.spark.base.SparkUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author userkdg
 * @date 2020/4/2 22:52
 **/
public class SparkStreamDefineReceicer {
    public volatile static boolean isStart = true;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test spark streaming");
        SparkSession spark = SparkUtils.getSpark(sparkConf);
        try (JavaStreamingContext jsc = SparkUtils.getSsc(spark, Durations.milliseconds(500L))) {
//            jsc.checkpoint(System.getProperty("user.dir")+"/tmp");
            Server server = new Server(8888);
            ContextHandler contextHandler = new ContextHandler();
            contextHandler.setContextPath("/stop/spark");
            contextHandler.setHandler(new CloseStreamHanlder(jsc));
            server.setHandler(contextHandler);
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }

            String checkPat = System.getProperty("user.dir") + "/checkpoint-tmp-streaming";
            jsc.checkpoint(checkPat);
            JavaReceiverInputDStream<String> dStream = jsc.receiverStream(
                    new Receiver<String>(StorageLevel.MEMORY_AND_DISK()) {
                        @Override
                        public void onStart() {
                            System.out.println("start");
                            int i = 0;
                            while (isStart) {
                                store("test" + (i++) + "," + RandomStringUtils.random(10, "utf-8")
                                        + "," + RandomStringUtils.random(10, "abcdefghigklm1234567890"));
                                try {
                                    TimeUnit.MILLISECONDS.sleep(500L);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void onStop() {
                            System.out.println("stop");
//                            spark.stop();
//                            jsc.stop(true, true);
                            closeBusData();
                        }
                    });
            dStream.cache().checkpoint(Durations.milliseconds(500L));
            JavaDStream<Iterator<String>> iteratorJavaDStream = dStream
                    .mapPartitions(new FlatMapFunction<Iterator<String>, Iterator<String>>() {
                        @Override
                        public Iterator<Iterator<String>> call(Iterator<String> stringIterator) throws Exception {
                            while (stringIterator.hasNext()) {
                                String s = stringIterator.next();
                                System.out.println("flatmap:" + s);
                            }
                            return null;
                        }
                    });
            iteratorJavaDStream.checkpoint(Durations.milliseconds(500L));
            iteratorJavaDStream.foreachRDD(new VoidFunction2<JavaRDD<Iterator<String>>, Time>() {
                @Override
                public void call(JavaRDD<Iterator<String>> v1, Time v2) throws Exception {
                    v1.flatMap(new FlatMapFunction<Iterator<String>, String>() {
                        @Override
                        public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                            return stringIterator;
                        }
                    }).foreachAsync(new VoidFunction<String>() {
                        @Override
                        public void call(String s) throws Exception {
                            System.out.println(s + ",time:" + v2);
                        }
                    });
                }
            });
            jsc.start();
            try {
                jsc.awaitTermination();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeBusData() {
        System.out.println("关闭前，处理完成本数据");
    }
}
