import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.immutable.Seq;

import java.io.DataInputStream;
import java.util.Iterator;
import java.util.List;

/**
 * @author userkdg
 * @date 2019/7/25 23:48
 **/
public class SparkJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test spark job").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.binaryFiles("/path/xx.gz", 1)
                .zipWithIndex().map(new Function<Tuple2<Tuple2<String, PortableDataStream>, Long>, Object>() {
            public Object call(Tuple2<Tuple2<String, PortableDataStream>, Long> v1) throws Exception {
                Tuple2<String, PortableDataStream> dataStreamTuple2 = v1._1();
                Long aLong = v1._2();
                String path = dataStreamTuple2._1();
                PortableDataStream portableDataStream = dataStreamTuple2._2();
                DataInputStream open = portableDataStream.open();


                return null;
            }
        }).cache();

        SparkSession spark = SparkSession.builder().appName("my Spark Session").getOrCreate();
        // $example on:generic_load_save_functions$
        Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
        // $example off:generic_load_save_functions$
        // $example on:manual_load_options$
        Dataset<Row> peopleDF =
                spark.read().format("json").load("examples/src/main/resources/people.json");
        //  people.filter("age > 30")
        // *     .join(department, people("deptId") === department("id"))
        // *     .groupBy(department("name"), people("gender"))
        // *     .agg(avg(people("salary")), max(people("age")))
        // * }}}
        usersDF.join(peopleDF, usersDF.col("id").equalTo(peopleDF.col("userId")))
                .as(Encoders.STRING());
        peopleDF.toJavaRDD().mapPartitions(new FlatMapFunction<Iterator<Row>, Iterator<List<String>>>() {
            public Iterator<Iterator<List<String>>> call(Iterator<Row> rowIterator) throws Exception {
                while (rowIterator.hasNext()){
                    Row next = rowIterator.next();
                    int size = next.size();
                    for (int i = 0; i < size; i++) {
                        Object o = next.get(i);
                    }
                }
                return null;
            }
        });

//        StructType schema = createStructType(new StructField[]{
//                createStructField("id", IntegerType, false),
//                createStructField("hour", IntegerType, false),
//                createStructField("mobile", DoubleType, false),
//                createStructField("userFeatures", new VectorUDT(), false),
//                createStructField("clicked", DoubleType, false)
//        });
    }
}
