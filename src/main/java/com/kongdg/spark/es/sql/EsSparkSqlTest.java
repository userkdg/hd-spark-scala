package com.kongdg.spark.es.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kongdg.spark.base.SparkUtils;
import com.kongdg.spark.es.pojo.EsSparkPo;
import com.kongdg.spark.es.pojo.TripBean;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author userkdg
 * @date 2020-04-06 14:47
 **/
public class EsSparkSqlTest {
    public static void main(String[] args) {


    }

    /**
     * 写入es
     * 写入es配置了"es.mapping.id", "id"后，要是id值与之前的文档id相等，则会覆盖前一个文档数据
     */
    @Test
    public void saveToEs() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").set("es.index.auto.create", "true");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            JavaSparkContext jsc = SparkUtils.getJScBySpark(sparkConf, spark);
            Map<String, ?> numbers = ImmutableMap.of("one", 4, "two", 3, "id", 1);
            Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni4", "SFO", "San Fran4", "id", 2);
            JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
            JavaEsSpark.saveToEs(javaRDD, "spark/docs", ImmutableMap.of("es.mapping.id", "id"));
        }
    }

    /**
     * 根据每一行的rdd的某一个字段的值进行分片，生成各自的索引，写入es
     */
    @Test
    public void saveToEs2() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").set("es.index.auto.create", "true");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            JavaSparkContext jsc = SparkUtils.getJScBySpark(sparkConf, spark);
            Map<String, ?> numbers = ImmutableMap.of("media_type", "shop", "one", 4, "two", 3, "id", 1);
            Map<String, ?> airports = ImmutableMap.of("media_type", "study", "OTP", "Otopeni4", "SFO", "San Fran4", "id", 2);
            Map<String, ?> game =
                    ImmutableMap.of("media_type", "game", "title", "FF VI", "year", "1994");
            JavaRDD<Map<String, ?>> javaRDD =
                    jsc.parallelize(ImmutableList.of(game, airports, numbers));
            ImmutableMap<String, String> cfg = ImmutableMap.of(
                    "spark.es.nodes", "127.0.0.1",
                    "spark.es.port", "9200",
                    "spark.es.nodes.wan.only", "false");
//            es.nodes.wan.only设置为true时即只通过client节点进行读取操作，
//            因此主节点负载会特别高，性能很差。
//            长时间运行后，java gc回收一次要几十秒，慢慢的OOM，系统崩溃。
//            new SparkConf().setAppName("writeEs").setMaster("local[*]").set("es.index.auto.create", "true")
//                    .set("es.nodes", "ELASTIC_SEARCH_IP").set("es.port", "9200").set("es.nodes.wan.only", "true");
            JavaEsSpark.saveToEs(javaRDD, "spark-collection-{media_type}/doc", cfg);
        }
    }

    /**
     * rdd的实体存在反射必须为public和序列化
     */
    @Test
    public void saveToEs3() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]").set("es.index.auto.create", "true");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            JavaSparkContext jsc = SparkUtils.getJScBySpark(sparkConf, spark);
            TripBean upcoming = new TripBean("OTP", "SFO");
            TripBean lastWeek = new TripBean("MUC", "OTP");
            JavaRDD<TripBean> javaRDD = jsc.parallelize(
                    ImmutableList.of(upcoming, lastWeek));
            JavaEsSpark.saveToEs(javaRDD, "spark/docs");
        }
    }

    /**
     * 1.不能用JavaPairRDD<String, Map<String, Object>> javaPairRDD = JavaEsSpark.esRDD(jsc);
     * List<Tuple2<String, Map<String, Object>>> take = javaPairRDD.take(10);
     * 获取所有
     * 2.不能用
     * <code>
     * Map<String, String> cfg = new HashMap<String, String>() {
     * private static final long serialVersionUID = 1L;
     * {
     * put(ConfigurationOptions.ES_RESOURCE, "spark/docs");
     * put(ConfigurationOptions.ES_NODES, "127.0.0.1");
     * put(ConfigurationOptions.ES_PORT, ConfigurationOptions.ES_PORT_DEFAULT);
     * //                put(ConfigurationOptions.ES_QUERY, "?q=中国");
     * put(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
     * }
     * }
     * </code>
     * 3.{@see ConfigurationOptions.ES_QUERY}可用post -d body 或 get ?q=_id:xxx
     * <p>
     * 4.读取es的数据返回rdd
     */
    @Test
    public void readEs() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            JavaSparkContext jsc = SparkUtils.getJScBySpark(sparkConf, spark);
            /**
             * JavaPairRDD<String, Map<String, Object>> javaPairRDD = JavaEsSpark.esRDD(jsc);
             *             List<Tuple2<String, Map<String, Object>>> take = javaPairRDD.take(10);
             *
             * org.elasticsearch.hadoop.EsHadoopIllegalArgumentException: invalid resource given; expecting [index]/[type] - received null
             */
            Map<String, String> cf2 = new HashMap<String, String>();
            cf2.put(ConfigurationOptions.ES_RESOURCE, "ik_index/docs");
            cf2.put(ConfigurationOptions.ES_NODES, "127.0.0.1");
            cf2.put(ConfigurationOptions.ES_PORT, ConfigurationOptions.ES_PORT_DEFAULT);
//            2 cfg.  put(ConfigurationOptions.ES_QUERY, "?q=fS1FTnEBjkoUhqmmkaxq");
            cf2.put(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
            JavaPairRDD<String, String> javaPairRDD = JavaEsSpark.esJsonRDD(jsc, cf2);
            List<Tuple2<String, String>> take = javaPairRDD.take(10);
            System.out.println(take);
    /*      以下构造会导致序列号内部类失败的情况！！
            Map<String, String> cfg = new HashMap<String, String>() {
                private static final long serialVersionUID = 1L;
                {
                    put(ConfigurationOptions.ES_RESOURCE, "spark/docs");
                    put(ConfigurationOptions.ES_NODES, "127.0.0.1");
                    put(ConfigurationOptions.ES_PORT, ConfigurationOptions.ES_PORT_DEFAULT);
//                put(ConfigurationOptions.ES_QUERY, "?q=中国");
                    put(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
                }
            };*/
            Map<String, String> cfg = new HashMap<>(16);
            cfg.put(ConfigurationOptions.ES_RESOURCE, "spark/docs");
            cfg.put(ConfigurationOptions.ES_NODES, "127.0.0.1");
            cfg.put(ConfigurationOptions.ES_PORT, ConfigurationOptions.ES_PORT_DEFAULT);
//              cfg.  put(ConfigurationOptions.ES_QUERY, "?q=_id:fS1FTnEBjkoUhqmmkaxq"); // ok
            cfg.put(ConfigurationOptions.ES_QUERY, "{\n" +
                    "  \"query\": {\n" +
                    "    \"term\": {\n" +
                    "      \"_id\": {\n" +
                    "        \"value\": \"fS1FTnEBjkoUhqmmkaxq\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"); // ok
            cfg.put(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, "true");
            JavaPairRDD<String, String> pairRDD = JavaEsSpark.esJsonRDD(jsc, cfg);
            JavaRDD<Tuple2<String, String>> map = pairRDD.rdd()
                    .toJavaRDD();
//            Dataset<Row> dataFrame = spark.createDataFrame(map, EsSparkPo.class);
//            dataFrame.printSchema();
//            dataFrame.show();

            System.out.println("===========================");
            List<Tuple2<String, String>> take1 = pairRDD.take(10);
            System.out.println(take1);
//            List<String> strings = Arrays.asList("OTP", "SFO", "arrival", "departure", "id", "one", "two");
//            Configuration conf = new Configuration();
//            首次目录不存在可行，多次append不行
//            pairRDD.saveAsHadoopFile("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\spark-hd-es-data\\text",
//                    LongWritable.class, Text.class,
//                    TextOutputFormat.class);
/*
           兼容性非常差 pass
            pairRDD.saveAsHadoopFile("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\spark-hd-es-data\\orc",
                    Text.class, Text.class,
                    OrcOutputFormat.class);

            pairRDD.saveAsHadoopFile("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\spark-hd-es-data\\parquet",
                    Void.class, Text.class,
                    DeprecatedParquetOutputFormat.class);*/

//            JavaPairRDD esRDD = jsc.hadoopRDD(conf, EsInputFormat.class, Text.class, MapWritable.class);
//            pairRDD.saveAsHadoopFile();
//            JavaPairRDD<String, String> pairRDD = JavaEsSpark.esJsonRDD(jsc, "spark/docs");
//            spark.createDataFrame()
        }
    }

    /**
     * 基于SQLContext返回dataframe
     */
    @Test
    public void readEs2() {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        try (SparkSession spark = SparkUtils.getSpark(sparkConf)) {
            JavaSparkContext jsc = SparkUtils.getJScBySpark(sparkConf, spark);
            SQLContext sql = new SQLContext(jsc);
            Dataset<Row> rowDataset = JavaEsSparkSQL.esDF(sql, "spark/docs");
            rowDataset.printSchema();
            rowDataset.show();
            rowDataset.write()
                    .option("path", "F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\spark-data")
                    .mode("Append")
                    .format("csv")
                    .save();
            val df = spark.sqlContext().read().format("org.elasticsearch.spark.sql").load("spark/docs");
            df.printSchema();
            df.show();

        }

    }


}
