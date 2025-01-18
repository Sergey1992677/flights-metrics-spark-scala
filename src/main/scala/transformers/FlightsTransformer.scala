package com.example.transformers

import com.example.constants.FlightsReadConstant
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object FlightsTransformer {

  val isCancelled = (record: Byte) => record == 1.toByte

  val isLateFlight = (row: Array[String]) => {

    row(FlightsReadConstant.elapsedTimeColPos).toInt > row(FlightsReadConstant.scheduledTimeColPos).toInt
  }

  val isLateArrival = (arrivalTime: Int) => arrivalTime > 0

  val hasBeenProcessedEarlier = (year: Int, month: Int, lastPeriodAnalyzed: Int) => lastPeriodAnalyzed > (year * 100 + month - 1)

  def aggRows(rdd: RDD[Array[String]], aggCol: Byte): RDD[(String, Long)] = rdd
    .map(row => (row(aggCol), 1L))
    .reduceByKey(_ + _)

  private val isDelayedFlight = (time: Int) => time > 0

  import scala.util.Try

  def getInTimeDepartures(rdd: RDD[Array[String]]) = {

    rdd
      .filter(row => {
        Try(row(FlightsReadConstant.departureDelayColPos).toInt).isSuccess
      })
      .filter(row => !isDelayedFlight(row(FlightsReadConstant.departureDelayColPos).toInt))
  }

  def calcDelayedReason(delayedMinutes: Long): Long = if (delayedMinutes > 0L) 1L else 0L

  def calcPercent(part: Long, total: Long): String = s"${"%.2f".format(part.toDouble / total.toDouble * 100.0)}%"

  def makeTopReportRDD(sc: SparkContext,
                       rdd: RDD[(String, Long)],
                       topN: Int,
                       metricName: String): RDD[(String, String, Long)] = {

    sc.parallelize(rdd.take(topN))
      .map(row => (metricName, row._1, row._2))
  }
}