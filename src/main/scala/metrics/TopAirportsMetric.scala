package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant, MetricsName, TopAirportsConstant}
import com.example.readers.{DataFrameParquetReader, HistoricalDataReader}
import com.example.schemas.{MetricsSchemas, ReadersSchemas, WritersSchemas}
import com.example.transformers.{AirDictsTransformer, FlightsTransformer}
import com.example.writers.{AnalyserFileWriter, ConsoleWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TopAirportsMetric(spark: SparkSession,
                        sc: SparkContext,
                        initFlightsRDD: RDD[Array[String]],
                        airportsMap: scala.collection.Map[String, String]
                       ) extends Metric {

  override def calculate(): Unit = {

    import spark.implicits._

    val airportsTmpMap = airportsMap

    val targetFolder = Config.get(Config.mainFolderKey)

    val historicalDataPath = s"${targetFolder}${FileConstant.historicalDataFolder}${FileConstant.topAirports}"

    val historicalRDD = HistoricalDataReader[(String, Long)](spark,
      sc, historicalDataPath, ReadersSchemas.topAirports, HistoricalDataReader.makeTuple2FromRow).read()

    val airportsOriginRDD = FlightsTransformer.aggRows(initFlightsRDD, FlightsReadConstant.airportOriginColPos)

    val airportsDestRDD = FlightsTransformer.aggRows(initFlightsRDD, FlightsReadConstant.airportDestColPos)

    val sortType = TopAirportsConstant.sortType

    val topN = TopAirportsConstant.topN

    val airportsRDD = airportsOriginRDD
      .union(airportsDestRDD)
      .union(historicalRDD)
      .reduceByKey(_ + _)
      .cache()

    val sortedAirportsRDD = airportsRDD
      .map(row => (AirDictsTransformer.getNameFromDict(row._1, airportsTmpMap), row._2))
      .sortBy(sortType * _._2)

    val metricName = MetricsName.getTopAirports(topN, sortType)

    val topAirportsRDD = FlightsTransformer.makeTopReportRDD(sc, sortedAirportsRDD, topN, metricName)

    val consoleWriter = new ConsoleWriter(spark)

    consoleWriter.printToConsole[(String, String, Long)](topAirportsRDD, MetricsSchemas.topAirports)

    val analyserFileWriter = new AnalyserFileWriter(spark)

    analyserFileWriter.writeRddToParquet[(String, Long)](airportsRDD,
      WritersSchemas.topAirports, historicalDataPath)
  }
}