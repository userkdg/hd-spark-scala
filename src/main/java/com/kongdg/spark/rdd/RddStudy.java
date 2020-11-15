package com.kongdg.spark.rdd;

import com.google.common.collect.Lists;
import com.kongdg.spark.base.BaseInit;
import com.kongdg.spark.base.SparkUtils;
import com.mysql.jdbc.Driver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.sql.*;
import java.util.Iterator;
import java.util.List;

/**
 * @author userkdg
 * @date 2020-05-29 12:42
 **/
public class RddStudy extends BaseInit {
    public static void main(String[] args) {
        try {
            Class.forName(Driver.class.getName());
            try (Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "0=rW-omsltEV")) {
                try (PreparedStatement ps = connection.prepareStatement("show databases")) {
                    try (ResultSet rs = ps.getResultSet()) {
                        System.out.println(rs);
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        SparkSession spark = SparkUtils.getSpark(conf);
        try (JavaSparkContext jsc = SparkUtils.getJScBySpark(spark)) {
            JavaRDD<Integer> rdd = jsc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7), 2);
            rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Tuple2<Integer,Integer>>() {
                @Override
                public Iterator<Tuple2<Integer, Integer>> call(Iterator<Integer> integerIterator) throws Exception {
                    List<Tuple2<Integer, Integer>> iterator = Lists.newArrayList();
                    integerIterator.forEachRemaining(i->{
                        iterator.add(Tuple2.apply(i,i));
                    });
                    return iterator.iterator();
                }
            });
            Integer aggregate = rdd.aggregate(0,
                    new Function2<Integer, Integer, Integer>() { // 分区的局部计算
                        @Override
                        public Integer call(Integer v1, Integer v2) throws Exception {
                            return Math.max(v1, v2);
//                            return v1 + v2;
                        }
                    }, new Function2<Integer, Integer, Integer>() {// 所有分区的计算
                        @Override
                        public Integer call(Integer v1, Integer v2) throws Exception {
                            return Math.max(v1, v2);
//                            return v1 + v2;
                        }
                    });
            System.out.println(aggregate);
            JavaRDD<Tuple2<String, Integer>> parallelize = jsc.parallelize(Lists.newArrayList(Tuple2.apply("cat", 2), Tuple2.apply("cat", 3), Tuple2.apply("mouse", 2)), 2);
            parallelize.treeAggregate(0, new Function2<Integer, Tuple2<String, Integer>, Integer>() {
                @Override
                public Integer call(Integer v1, Tuple2<String, Integer> v2) throws Exception {
                    return null;
                }
            }, new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return null;
                }
            }, 2);

//            rdd.aggregate()
//            rdd.groupBy()
            JavaRDD<Integer> repartition = rdd.sortBy(new Function<Integer, Integer>() {
                @Override
                public Integer call(Integer v1) throws Exception {
                    return v1;
                }
            }, false, 3)
                    .coalesce(4, true)
                    .repartition(5);
            
            JavaRDD<String> stringJavaRDD = jsc.textFile("hdfs://anonymous:9000/flume/events/20-05-28/1220/events-.1590639650582");
            List<String> collect = stringJavaRDD.collect();
            System.out.println(collect);
        }
    }
}
