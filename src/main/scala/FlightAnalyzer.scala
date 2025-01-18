package com.example

import com.example.configs.Config
import com.example.jobs._
import com.example.metrics.MetricsProvider
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait AirContext {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(Config.appName)
    .master(Config.get(Config.appMode))
    .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext
}

object FlightAnalyzer extends AirContext{

  def main(args: Array[String]): Unit = {

    val metaInfoReadJob = new MetaInfoReadJob(sc)

    val flightsReadJob = new FlightsReadJob(sc)

    val airportsReadJob = new AirportsReadJob(sc)

    val airlinesReadJob = new AirlinesReadJob(sc)

    val metricsProvider = new MetricsProvider(
      spark,
      sc,
      metaInfoReadJob,
      flightsReadJob,
      airportsReadJob,
      airlinesReadJob
    )

    metricsProvider.calcMetrics()

    spark.stop()
  }
}