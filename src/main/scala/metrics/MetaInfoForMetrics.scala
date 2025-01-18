package com.example.metrics

import com.example.configs.Config
import com.example.constants.{FlightsReadConstant, MetaInfoConstant}
import com.example.schemas.WritersSchemas
import com.example.writers.AnalyserFileWriter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.{LocalDate, Month}
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale
import java.time.LocalDate.{now => todayDate}

object MetaInfoForMetrics {

  private val getMonthName = (month: Int) => Month.of(month).getDisplayName(TextStyle.FULL_STANDALONE,
    Locale.forLanguageTag(Config.languageForDisplay))

  private val makePeriodString = (month: Int,
                                  year: Int) => s"${getMonthName(month)} ${year}${MetaInfoConstant.metaPostfix}"

  private val getTopYearAndMonth = (rdd: RDD[((Int, Int), Long)], sortType: Boolean) => {

    rdd
      .sortByKey(ascending = sortType)
      .first()
  }

  private val formatDate = (dateToFormat: LocalDate) => {

    val fmt = DateTimeFormatter.ofPattern(MetaInfoConstant.dateFormat)

    dateToFormat.format(fmt)
  }
}

class MetaInfoForMetrics(spark: SparkSession,
                         sc: SparkContext,
                         initFlightsRDD: RDD[Array[String]],
                         metaLastYear: Int,
                         metaLastMonth: Int,
                         metaFirstYear: Int,
                         metaFirstMonth: Int
                        ) {

  def logInfo(): Unit = {

    val metaInfoRDD = sc.parallelize(
      List(((metaLastYear, metaLastMonth), 0L),
        ((metaFirstYear, metaFirstMonth), 0L))
    )
      .filter(0 != _._1._2)

    val periodRDD = initFlightsRDD
      .map(row => ((row(FlightsReadConstant.yearColPos).toInt, row(FlightsReadConstant.monthColPos).toInt), 0L))
      .reduceByKey(_ + _)
      .union(metaInfoRDD)

    val ((firstYear: Int, firstMonth: Int), _) = MetaInfoForMetrics.getTopYearAndMonth(periodRDD, true)

    val ((lastYear: Int, lastMonth: Int), _) = MetaInfoForMetrics.getTopYearAndMonth(periodRDD, false)

    val firstPeriod = MetaInfoForMetrics.makePeriodString(firstMonth, firstYear)

    val lastPeriod = MetaInfoForMetrics.makePeriodString(lastMonth, lastYear)

    val metaRecordList = List(s"${firstPeriod} - ${lastPeriod}")

    val todayDateList = List(MetaInfoForMetrics.formatDate(todayDate))

    val beginAndEndPeriodList = List(firstMonth, firstYear, lastMonth, lastYear).map(_.toString)

    val rows: List[List[String]] = List(metaRecordList ++ todayDateList ++ beginAndEndPeriodList)

    val targetFolder = Config.get(Config.mainFolderKey)

    val fileFullPath = s"${targetFolder}${MetaInfoConstant.fileName}"

    val analyserFileWriter = new AnalyserFileWriter(spark)

    analyserFileWriter.writeCsvFile(fileFullPath, WritersSchemas.metaInfo, rows, MetaInfoConstant.isAppended)
  }
}