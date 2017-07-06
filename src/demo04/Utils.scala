package demo04

import org.apache.spark.{SparkContext,SparkConf}

/**
 * Created by fansy on 2017/7/6.
 */
object Utils {

  /**
   * 获得sc
   * @param master
   * @param appName
   * @param jars
   * @return
   */
  def getSc(master:String,appName:String,jars:Array[String],logEnabled:String,logDir:String):SparkContext = {
    val conf = new SparkConf().setMaster(master).setAppName(appName).setJars(jars)
      .set("spark.eventLog.enabled",logEnabled)
      .set("spark.eventLog.dir",logDir)
    new SparkContext(conf)
  }


}
