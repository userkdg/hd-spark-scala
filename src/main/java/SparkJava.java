import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.List;

/**
 * @author userkdg
 * @date 2019/7/25 23:48
 **/
public class SparkJava {
    static {
        String checkPat = "D:\\bigdata\\hadoop-2.7.2";
        System.setProperty("HADOOP_HOME", checkPat);
        System.setProperty("hadoop.home.dir", checkPat);

    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[1]").appName("my Spark Session").getOrCreate();
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
                while (rowIterator.hasNext()) {
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
