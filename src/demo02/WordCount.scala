package demo02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by fansy on 2017/7/5.
 */
object WordCount {
  def main(args: Array[String]) {

    val input = "hdfs://192.168.128.128:8020/user/root/magic"
    val output =""

    val appName = "word count"
    val master = "spark://192.168.128.128:7077"
    val jars =Array("C:\\Users\\fansy\\workspace_idea_tmp\\JavaConnectSaprk01\\out\\artifacts\\wordcount\\wordcount.jar")

    val conf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars)
      .set("spark.eventLog.enabled","true")
    .set("spark.eventLog.dir","hdfs://node10:8020/eventLog")
    .set("spark.driver.host","192.168.128.128")
    .set("spark.driver.port","8993")
    val sc = new SparkContext(conf)

    val line = sc.textFile(input)

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }
}
