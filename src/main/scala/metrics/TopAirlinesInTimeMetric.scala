package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant, MetricsName, TopAirlinesConstant}
import com.example.readers.{DataFrameParquetReader, HistoricalDataReader}
import com.example.schemas.{MetricsSchemas, ReadersSchemas, WritersSchemas}
import com.example.transformers.{AirDictsTransformer, FlightsTransformer}
import com.example.writers.{AnalyserFileWriter, ConsoleWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Try

class TopAirlinesInTimeMetric(spark: SparkSession,
                              sc: SparkContext,
                              initFlightsRDD: RDD[Array[String]],
                              airlinesMap: scala.collection.Map[String, String]
                             ) extends Metric {

  override def calculate(): Unit = {

    import spark.implicits._

    val targetFolder = Config.get(Config.mainFolderKey)

    val historicalDataPath = s"${targetFolder}${FileConstant.historicalDataFolder}${FileConstant.topAirlinesInTime}"

    val historicalRDD = HistoricalDataReader[(String, Long)](spark,
      sc, historicalDataPath, ReadersSchemas.topAirlinesInTime, HistoricalDataReader.makeTuple2FromRow).read()

    val airlinesTmpMap = airlinesMap

    val filghtsRDD = initFlightsRDD
      .filter(row => {
        Try(row(FlightsReadConstant.elapsedTimeColPos).toInt).isSuccess &
          Try(row(FlightsReadConstant.scheduledTimeColPos).toInt).isSuccess
      })
      .filter(!FlightsTransformer.isLateFlight(_))

    val sortType = TopAirlinesConstant.sortType

    val topN = TopAirlinesConstant.topN

    val airlinesInTimeRDD = FlightsTransformer
      .aggRows(filghtsRDD, FlightsReadConstant.airlineIataCodeColPos)
      .union(historicalRDD)
      .reduceByKey(_ + _)
      .cache()

    val sortedAirlinesInTineRDD = airlinesInTimeRDD
      .map(row => (AirDictsTransformer.getNameFromDict(row._1, airlinesTmpMap), row._2))
      .sortBy(sortType * _._2)

    val metricName = MetricsName.getTopAirlines(topN, sortType)

    val topAirLinesRDD = FlightsTransformer.makeTopReportRDD(sc, sortedAirlinesInTineRDD, topN, metricName)

    val consoleWriter = new ConsoleWriter(spark)

    consoleWriter.printToConsole[(String, String, Long)](topAirLinesRDD,  MetricsSchemas.topAirlines)

    val analyserFileWriter = new AnalyserFileWriter(spark)

    analyserFileWriter.writeRddToParquet[(String, Long)](airlinesInTimeRDD,
      WritersSchemas.topAirlinesInTime, historicalDataPath)
  }
}