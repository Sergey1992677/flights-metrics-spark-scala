package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant, MetricsName, TopAirportsDestByAirportConstant}
import com.example.readers.{DataFrameParquetReader, HistoricalDataReader}
import com.example.schemas.{MetricsSchemas, ReadersSchemas, WritersSchemas}
import com.example.transformers.{AirDictsTransformer, FlightsTransformer, RankCalcTransformer}
import com.example.writers.{AnalyserFileWriter, ConsoleWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TopAirportsDestByAirportMetric(spark: SparkSession,
                                     sc: SparkContext,
                                     initFlightsRDD: RDD[Array[String]],
                                     airportsMap: scala.collection.Map[String, String]
                                    ) extends Metric {

  override def calculate(): Unit = {

    import spark.implicits._

    val airportsTmpMap = airportsMap

    val targetFolder = Config.get(Config.mainFolderKey)

    val historicalDataPath = s"${targetFolder}${FileConstant.historicalDataFolder}${FileConstant.topAirportsDestByAirport}"

    val historicalRDD = HistoricalDataReader[Array[String]](spark,
      sc, historicalDataPath, ReadersSchemas.topAirportsDestByAirport, HistoricalDataReader.makeArrayFromRow).read()

    val sortType = TopAirportsDestByAirportConstant.sortType

    val filghtsRDD = FlightsTransformer
      .getInTimeDepartures(initFlightsRDD)
      .map(row => Array(row(FlightsReadConstant.airportOriginColPos), row(FlightsReadConstant.airportDestColPos)))

    val fullFilghtsRDD = filghtsRDD
      .union(historicalRDD)
      .cache()

    val aggFlightsRDD = RankCalcTransformer.aggForRank(
      fullFilghtsRDD,
      0.toByte,
      1.toByte,
      sortType
    )

    val topN = TopAirportsDestByAirportConstant.topN + 1L

    val metricName = MetricsName.getTopAirportsDestByAirport(topN - 1L, sortType)

    val rankRDD = RankCalcTransformer
      .calcRank(aggFlightsRDD, sc)
      .filter(_._1 < topN)
      .map(
        row => (metricName,
          AirDictsTransformer.getNameFromDict(row._2._2, airportsTmpMap),
          AirDictsTransformer.getNameFromDict(row._2._3, airportsTmpMap),
          row._1, Math.abs(row._2._1))
      )

    val consoleWriter = new ConsoleWriter(spark)

    consoleWriter.printToConsole[(String, String, String, Long, Long)](rankRDD,
      MetricsSchemas.topAirportsDestByAirport)

    val analyserFileWriter = new AnalyserFileWriter(spark)

    val fullFilghtsToWriteRDD = fullFilghtsRDD
      .map(row => (row(0), row(1)))

    analyserFileWriter.writeRddToParquet[(String, String)](fullFilghtsToWriteRDD,
      WritersSchemas.topAirportsDestByAirport, historicalDataPath)
  }
}