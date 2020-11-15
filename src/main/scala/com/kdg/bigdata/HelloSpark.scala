//package com.kdg.bigdata
//
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//
//object HelloSpark {
//  def main(args: Array[String]): Unit = {
//    val spark=SparkSession.builder().master("local[*]").getOrCreate()
//
//    val sc = spark.sparkContext
//    val textFile = sc.textFile("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\test-flink.txt")
//    val counts = textFile.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .collect()
//    val counts2 = textFile.flatMap(_.split(" "))
//      .map(a => (a, 1))
//      .collect()
//    counts.foreach(a=>println(a._1+":"+a._2))
//  }
//
//}
