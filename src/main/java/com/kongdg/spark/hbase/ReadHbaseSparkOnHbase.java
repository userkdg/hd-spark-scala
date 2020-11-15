package com.kongdg.spark.hbase;

import com.kongdg.spark.base.BaseInit;
import com.kongdg.spark.base.SparkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @author userkdg
 * @date 2020-05-27 23:07
 **/
public class ReadHbaseSparkOnHbase extends BaseInit {
    public static void main(String[] args) {

        Configuration config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\hbase", "hbase-site.xml"));
        config.addResource(new Path("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\hbase", "core-site.xml"));
        config.set("zookeeper.znode.parent","/test");
        config.set(TableInputFormat.INPUT_TABLE, "test");


        SparkConf conf = new SparkConf().setAppName("test spark job").setMaster("local[*]");
        SparkSession spark = SparkUtils.getSpark(conf);
        JavaSparkContext jsc = SparkUtils.getJScBySpark(spark);
        JavaPairRDD<ImmutableBytesWritable, Result> javaRDD = jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        javaRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
            @Override
            public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()){
                    Tuple2<ImmutableBytesWritable, Result> next = tuple2Iterator.next();
                    System.out.println(next);
                }
            }
        });
//        val sc = new SparkContext("local", "test");
//        val config = new HBaseConfiguration();
//        val hbaseContext = new HBaseContext(sc, config);
//
//        rdd.hbaseForeachPartition(hbaseContext, (it, conn) => {
//            val bufferedMutator = conn.getBufferedMutator(TableName.valueOf("t1"))
//            it.foreach((putRecord) => {
//                    . val put = new Put(putRecord._1)
//                    . putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
//. bufferedMutator.mutate(put)
// })
//            bufferedMutator.flush()
//            bufferedMutator.close()
//        })
    }
}
