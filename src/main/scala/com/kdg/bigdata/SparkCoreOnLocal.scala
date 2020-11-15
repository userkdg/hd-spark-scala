//package com.kdg.bigdata
//
//import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
//
//object SparkCoreOnLocal {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2")
//    val spark = SparkSession.builder().master("local[*]").getOrCreate()
//    val df = spark.sparkContext.textFile("F:\\j_workspace\\hd-spark-scala\\src\\main\\resources\\test-flink.txt")
//    val line = df.flatMap(r => r.toString.split(" "))
//    val wc=line.map(w=>(w,1))
//    val wcc =wc.reduceByKey(_+_)
//    wcc.foreach(wordcount=>println(wordcount._1+":"+wordcount._2))
//  }
//
//
//}
