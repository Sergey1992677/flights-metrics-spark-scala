package com.example.writers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Encoder}

class ConsoleWriter(spark: SparkSession) {

  import spark.implicits._

  def printToConsole[T: Encoder](rdd: RDD[T],
                                 dfSchema: Array[String]): Unit = {

    rdd
      .toDF(dfSchema: _*)
      .show(50, truncate = false)
  }
}