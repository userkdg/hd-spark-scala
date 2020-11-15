//package com.kdg.bigdata
//
//import com.mongodb.spark.MongoSpark
//import org.apache.spark.SparkContext
//import org.bson.Document
//
//class MongoSparkTest {
//  def main(args: Array[String]): Unit = {
//
//  }
//
//  def read(sc: SparkContext): Unit = {
//    val rdd = MongoSpark.load(sc).withPipeline(Seq(Document.parse("{ $match:{name:'kdg'}")))
//    rdd.intersection(rdd).distinct().foreach(println(_))
//
//
//    val df = rdd.toDF()
//    df.foreach(r=>r[0])
//  }
//
//  def write(sc: SparkContext): Unit ={
////    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
////    MongoSpark.save(documents) // Uses the SparkConf for configuratio
//  }
//}
