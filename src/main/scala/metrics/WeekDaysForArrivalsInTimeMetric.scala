package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant, MetricsName, WeekDaysForArrivalsInTimeConstant}
import com.example.readers.{DataFrameParquetReader, HistoricalDataReader}
import com.example.schemas.{MetricsSchemas, ReadersSchemas, WritersSchemas}
import com.example.transformers.FlightsTransformer
import com.example.writers.{AnalyserFileWriter, ConsoleWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

class WeekDaysForArrivalsInTimeMetric(spark: SparkSession,
                                      sc: SparkContext,
                                      initFlightsRDD: RDD[Array[String]]
                                     ) extends Metric {

  override def calculate(): Unit = {

    import spark.implicits._

    val targetFolder = Config.get(Config.mainFolderKey)

    val historicalDataPath = s"${targetFolder}${FileConstant.historicalDataFolder}${FileConstant.weekDaysForArrivalsInTime}"

    val historicalRDD = HistoricalDataReader[(String, Long)](spark,
      sc, historicalDataPath, ReadersSchemas.weekDaysForArrivalsInTime, HistoricalDataReader.makeTuple2FromRow).read()

    val flightsRDD = initFlightsRDD
      .filter(row => Try(row(FlightsReadConstant.arrivalDelayColPos).toInt).isSuccess)
      .filter(row => !FlightsTransformer.isLateArrival(row(FlightsReadConstant.arrivalDelayColPos).toInt))

    val sortType = WeekDaysForArrivalsInTimeConstant.sortType

    val daysOfWeekRDD = FlightsTransformer
      .aggRows(flightsRDD, FlightsReadConstant.dayOfWeekColPos)
      .union(historicalRDD)
      .reduceByKey(_ + _)
      .cache()

    val metricName = MetricsName.getWeekDaysForArrivalsInTime(sortType)

    val sortedDaysOfWeekRDD = daysOfWeekRDD
      .map(row => (
        WeekDaysForArrivalsInTimeConstant.getNameOfWeekDay(row._1.toInt).toString,
        row._2
      ))
      .sortBy(sortType * _._2)
      .map(row => (metricName, row._1, row._2))

    val consoleWriter = new ConsoleWriter(spark)

    consoleWriter.printToConsole[(String, String, Long)](sortedDaysOfWeekRDD,
      MetricsSchemas.weekDaysForArrivalsInTime)

    val analyserFileWriter = new AnalyserFileWriter(spark)

    analyserFileWriter.writeRddToParquet[(String, Long)](daysOfWeekRDD,
      WritersSchemas.weekDaysForArrivalsInTime, historicalDataPath)
  }
}