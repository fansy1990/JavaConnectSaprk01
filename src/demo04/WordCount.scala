package demo04

import java.util.concurrent.Callable

import org.apache.spark.SparkContext

/**
 * Created by fansy on 2017/7/6.
 */
class WordCount[Boolean](sc:SparkContext,args:Array[String]) extends Callable[java.lang.Boolean]{

  override def call(): java.lang.Boolean = {
    try {
      // 处理参数
      val input = args(0)
      val output = args(1)

      // 业务逻辑
      val line = sc.textFile(input)
      println("line Number :" +line.count())
      line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

      // 关闭资源
      sc.stop()

      // 定义返回值
      true
    }catch {
      case e:Exception => false
    }
  }
}
