package demo03

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by fansy on 2017/7/5.
 */
object WordCount {

  def run(args:Array[String]):Boolean ={
    if(args.length!=7){
      println("demo.WordCount <input> <output> <appName> <master>" +
        " <jars> <logEnabled> <logDir>")
      System.exit(-1)
    }
    val input = args(0)
    val output = args(1)
    val appName = args(2)
    val master = args(3)
    val jars = args(4).split(",")
    val logEnabled = args(5)
    val logDir = args(6)

    try{
      val conf = new SparkConf().setAppName(appName).setMaster(master).setJars(jars)
        .set("spark.eventLog.enabled",logEnabled)
        .set("spark.eventLog.dir",logDir)
      println("line 49")
      val sc = new SparkContext(conf)
      println("id:"+sc.applicationId)
      println(sc.startTime)
      val line = sc.textFile(input)
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

      sc.stop()
      true
    }catch {
      case e:Exception => false
    }
  }
}
