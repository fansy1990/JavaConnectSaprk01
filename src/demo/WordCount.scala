package demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fansy on 2017/7/5.
 */
object WordCount {
  def main(args: Array[String]) {

    val input = "hdfs://192.168.128.128:8020/user/root/magic"
    val output =""

    val appName = "word count"
    val master = "spark://192.168.128.128:7077"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    val line = sc.textFile(input)

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }
}
