package com.example.metrics

import com.example.constants.FlightsReadConstant
import com.example.jobs._
import com.example.transformers.FlightsTransformer.{isCancelled, hasBeenProcessedEarlier}
import com.example.metrics._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MetricsProvider(spark: SparkSession,
                      sc: SparkContext,
                      metaInfoReadJob: MetaInfoReadJob,
                      flightsReadJob: FlightsReadJob,
                      airportsReadJob: AirportsReadJob,
                      airlinesReadJob: AirlinesReadJob
                    ) {

  def calcMetrics(): Unit = {

    val Array(lastPeriodAnalyzed, firstMonth, firstYear, lastMonth, lastYear) = metaInfoReadJob.read()

    val flightsRDD = flightsReadJob.read()

    val airportsMap = airportsReadJob.read()

    val airlinesMap = airlinesReadJob.read()

    val initFlightsRDD = flightsRDD
      .filter(row => {
        !isCancelled(row(FlightsReadConstant.cancelledColPos).toByte) &
          !hasBeenProcessedEarlier(row(FlightsReadConstant.yearColPos).toInt,
            row(FlightsReadConstant.monthColPos).toInt, lastPeriodAnalyzed)
      })
      .cache()

    List(
      new TopAirportsMetric(spark, sc, initFlightsRDD, airportsMap),
      new TopAirlinesInTimeMetric(spark, sc, initFlightsRDD, airlinesMap),
      new TopAirlinesByAirportMetric(spark, sc, initFlightsRDD, airlinesMap, airportsMap),
      new TopAirportsDestByAirportMetric(spark, sc, initFlightsRDD, airportsMap),
      new WeekDaysForArrivalsInTimeMetric(spark, sc, initFlightsRDD),
      new DelayedFlightsReasonsMetric(spark, sc, initFlightsRDD)
    )
      .map(_.calculate())

    new MetaInfoForMetrics(spark, sc, initFlightsRDD, lastYear, lastMonth, firstYear, firstMonth).logInfo()
  }
}