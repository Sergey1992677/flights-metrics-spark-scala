package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant, MetricsName}
import com.example.readers.{DataFrameParquetReader, HistoricalDataReader}
import com.example.schemas.{MetricsSchemas, ReadersSchemas, WritersSchemas}
import com.example.transformers.FlightsTransformer
import com.example.writers.{ConsoleWriter, AnalyserFileWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class DelayedFlightsReasonsMetric(spark: SparkSession,
                                  sc: SparkContext,
                                  initFlightsRDD: RDD[Array[String]]
                                 ) extends Metric {

  override def calculate(): Unit = {

    import spark.implicits._

    val targetFolder = Config.get(Config.mainFolderKey)

    val historicalDataPath = s"${targetFolder}${FileConstant.historicalDataFolder}${FileConstant.delayedFlightsReasons}"

    val historicalRDD = HistoricalDataReader[(Long, Long, Long, Long, Long)](spark,
      sc, historicalDataPath, ReadersSchemas.delayedFlightsReasons, HistoricalDataReader.makeTuple5FromRow).read()

    val fullFlightsRDD = initFlightsRDD
      .filter(_.length > (FlightsReadConstant.airSystemDelayColPos.toInt - 1))
      .map(
        row => (
          row(FlightsReadConstant.airSystemDelayColPos).toLong,
          row(FlightsReadConstant.securityDelayColPos).toLong,
          row(FlightsReadConstant.airlineDelayColPos).toLong,
          row(FlightsReadConstant.lateAircraftDelayColPos).toLong,
          row(FlightsReadConstant.weatherDelayColPos).toLong
        )
      )
      .union(historicalRDD)
      .cache()

    val delayedReasons = fullFlightsRDD
      .map(
        row => {
          val airSystemDelayMinutes = row._1
          val securituDelayMinutes = row._2
          val airlineDelayMinutes = row._3
          val lateAircraftDelayMinutes = row._4
          val weatherDelayMinutes = row._5
          val totalDelayMinutes = airSystemDelayMinutes + securituDelayMinutes +
            airlineDelayMinutes + lateAircraftDelayMinutes + weatherDelayMinutes

          (
            FlightsTransformer.calcDelayedReason(airSystemDelayMinutes),
            FlightsTransformer.calcDelayedReason(securituDelayMinutes),
            FlightsTransformer.calcDelayedReason(airlineDelayMinutes),
            FlightsTransformer.calcDelayedReason(lateAircraftDelayMinutes),
            FlightsTransformer.calcDelayedReason(weatherDelayMinutes),
            airSystemDelayMinutes,
            securituDelayMinutes,
            airlineDelayMinutes,
            lateAircraftDelayMinutes,
            weatherDelayMinutes,
            totalDelayMinutes
          )
        }
      )
      .reduce((a, b) => (
        a._1 + b._1,
        a._2 + b._2,
        a._3 + b._3,
        a._4 + b._4,
        a._5 + b._5,
        a._6 + b._6,
        a._7 + b._7,
        a._8 + b._8,
        a._9 + b._9,
        a._10 + b._10,
        a._11 + b._11
      ))

    val delayedReasonsInFlightsRDD = sc.parallelize(List((
      MetricsName.delayedReasonsMetric,
      delayedReasons._1.toString,
      delayedReasons._2.toString,
      delayedReasons._3.toString,
      delayedReasons._4.toString,
      delayedReasons._5.toString)))

    val delayedReasonsInPercentRDD = sc.parallelize(List({
      val totalMinutes = delayedReasons._11
      (
        MetricsName.delayedReasonsPercentMetric,
        FlightsTransformer.calcPercent(delayedReasons._6, totalMinutes),
        FlightsTransformer.calcPercent(delayedReasons._7, totalMinutes),
        FlightsTransformer.calcPercent(delayedReasons._8, totalMinutes),
        FlightsTransformer.calcPercent(delayedReasons._9, totalMinutes),
        FlightsTransformer.calcPercent(delayedReasons._10, totalMinutes)
      )
    }))

    val delayedReasonsRDD = delayedReasonsInFlightsRDD.union(delayedReasonsInPercentRDD)

    val consoleWriter = new ConsoleWriter(spark)

    consoleWriter.printToConsole[(String, String, String, String, String, String)](delayedReasonsRDD,
      MetricsSchemas.delayedFlightsReasons)

    val analyserFileWriter = new AnalyserFileWriter(spark)

    analyserFileWriter.writeRddToParquet[(Long, Long, Long, Long, Long)](fullFlightsRDD,
      WritersSchemas.delayedFlightsReasons, historicalDataPath)
  }
}