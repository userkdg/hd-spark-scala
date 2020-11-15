package com.kongdg.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.immutable.Seq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @author userkdg
 * @date 2020/4/4 11:45
 **/
public class SparkCoreOnLocalTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
//        为了满足hadoop在windows上有权限操作 winutils.exe 否则会导致写入文件等报权限不足
        System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
        try (SparkSession spark = SparkSession.builder().config(sparkConf).appName("test").master("local[1]").getOrCreate()) {
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//            spark.sql("").toDF();
            Dataset<Row> rowDataset = spark.createDataFrame(Arrays.asList(1, 2, 3, 4), Row.class);
            System.out.println();
            // ========== spark.createDataFrame()==========
            JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(Tuple2.apply(1, "kdg1"), Tuple2.apply(2, "kdg2"), Tuple2.apply(3, "kdg3")))
                    .map(new Function<Tuple2<Integer, String>, Row>() {
                        @Override
                        public Row call(Tuple2<Integer, String> tuple2) throws Exception {
                            return RowFactory.create(tuple2._1(),tuple2._2());
                        }
                    });
            StructField id = StructField.apply("id", DataTypes.IntegerType, true, Metadata.empty());
            StructField name = StructField.apply("name", DataTypes.StringType, true, Metadata.empty());
            StructType structType = new StructType(new StructField[]{id, name});
            Dataset<Row> dataFrame = spark.createDataFrame(rdd, structType);

            dataFrame.printSchema();
            dataFrame.show(10, 1, false);
//            Shell.execCommand("c")
            String parquetOutpat = "F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\orc";
            dataFrame.write().mode("Append").format("orc").save(parquetOutpat);

            Dataset<Row> parquet = spark.read().parquet("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\parquet\\*.parquet");
            parquet.show();
            long count = parquet.count();
            System.out.println(count + "条");
//            String path = url.getPath().substring(1);
            String path = "F:/j_workspace/hd-spark-scala/target/classes/result.csv";
            path = "F:/j_workspace/hd-spark-scala/target/classes/json.json";
            System.out.println(path);
            Path pathOk = Paths.get(path);
            System.out.println(pathOk);
            /*
            csv/json存在时间类型的文件需要增加timestampFormat,具体底层实现
             */
            Dataset<Row> csv = spark.read()
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
//                    .option("header", "true")
                    .json(pathOk.toString());
//                    .text(pathOk.toString());
            csv.show();

            Dataset<Row> df = spark.read().text("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\test-flink.txt");
            //a b c
            //1 2 3
            //1 b a
//            df.flatMap(new FlatMapFunction<Row, Object>() {
//                @Override
//                public Iterator<Object> call(Row row) throws Exception {
//                    return null;
//                }
//            });
            df.toJavaRDD()
                    .flatMapToPair((PairFlatMapFunction<Row, String, Integer>) row -> {
                        List<Tuple2<String, Integer>> tuple2s = Lists.newArrayList();
                        for (int i = 0; i < row.size(); i++) {
                            String r = Objects.toString(row.get(i), "");
                            String[] split = r.split(" ");
                            for (String s : split) {
                                tuple2s.add(Tuple2.apply(s, 1));
                            }
                        }
                        return tuple2s.iterator();
                    })
                    .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                    .foreach((VoidFunction<Tuple2<String, Integer>>) tuple2 ->
                            System.out.println(tuple2._1() + ":" + tuple2._2())
                    );
        }
    }
}
