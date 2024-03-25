package com.berkozmen
package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCtx {

  val SPARK: SparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()

  def sc: SparkContext = {
    val scI = new SparkContext(new SparkConf().setAppName("test").setMaster("local")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.driver.host", "localhost")
      .set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.cores", "1")
      .set("spark.executor.cores", "1"))
    scI.setLogLevel("WARN")
    scI
  }

}
