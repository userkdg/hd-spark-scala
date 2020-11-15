package com.kongdg.spark;

import com.clearspring.analytics.util.Lists;
import com.kongdg.spark.base.SparkUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author userkdg
 * @date 2020-04-25 22:22
 **/
public class TestSuanZiSpark implements Serializable{
// implements Serializable
    static {
        System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
//        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    }

    @Test
    public void testSparkSession(){
        SparkConf sparkConf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[*]");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            try (JavaSparkContext jsc = SparkUtils.getJScBySpark(spark)) {
                String checkPat = System.getProperty("user.dir") + "/checkpoint-tmp";
                jsc.setCheckpointDir(checkPat);
                JavaRDD<Integer> javaRDD = jsc.parallelize(Arrays.asList(1, 3, 4, 5, 5, 6));
                JavaRDD<Row> idJavaRdd = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Row>() {
                    @Override
                    public Iterator<Row> call(Iterator<Integer> integerIterator) throws Exception {
                        List<Row> structFields = Lists.newArrayList();
                        integerIterator.forEachRemaining(integer -> {
                            StructField id = DataTypes.createStructField("id", DataTypes.IntegerType, true);
                            structFields.add(RowFactory.create(id));
                        });
                        return structFields.iterator();
                    }
                });
//                StructType structType = DataTypes.createStructType(idJavaRdd.collect());
//                Dataset<Row> df = spark.createDataFrame(idJavaRdd, structType);
//                df.show();
            }
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            try (JavaSparkContext jsc = SparkUtils.getJScBySpark(spark)) {
                String checkPat = System.getProperty("user.dir")+"/checkpoint-tmp";
                jsc.setCheckpointDir(checkPat);
                JavaRDD<Integer> javaRDD = jsc.parallelize(Arrays.asList(1, 3, 4, 5, 5, 6));
                JavaPairRDD<Long, Integer> pairRDD = javaRDD.keyBy(new Function<Integer, Long>() {
                    @Override
                    public Long call(Integer v1) throws Exception {
                        return Long.valueOf(v1);
                    }
                });
                // transform
                /*
                窄依赖：map,mapPartitions.mapPartitionsWithIndex,filter,flatMap,keyBy,
                宽依赖: groupByKey,reduceByKey,sortByKey,sortBy
                 */
                JavaPairRDD<Long, Integer> cachePairRDD = pairRDD.cache();
                cachePairRDD.checkpoint();
                JavaPairRDD<Long, Integer> cachePairRDD2 = cachePairRDD.filter(new Function<Tuple2<Long, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Integer> v1) throws Exception {
                        return !v1._1().equals(1L);
                    }
                });
                cachePairRDD2.checkpoint();
                cachePairRDD2.foreachPartition(new VoidFunction<Iterator<Tuple2<Long, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<Long, Integer>> iterator) throws Exception {
                        iterator.forEachRemaining(t -> {
                            System.out.println("print: " + t._1() + ":" + t._2());
                        });
                    }
                });

                JavaRDD<Object> rdd = jsc.checkpointFile("F:\\j_workspace\\hd-spark-scala\\checkpoint-tmp\\d34aa17a-f842-47ca-9190-6cfa81319a69\\rdd-1");
                JavaRDD<Tuple2<Long, Integer>> mapRDD = rdd.map(o -> (Tuple2<Long, Integer>) o);
                mapRDD.foreach(new VoidFunction<Tuple2<Long, Integer>>() {
                    @Override
                    public void call(Tuple2<Long, Integer> t) throws Exception {
                        System.out.println("print: " + t._1() + ":" + t._2());
                    }
                });
            }
        }

    }
}
