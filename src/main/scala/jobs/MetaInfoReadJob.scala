package com.example.jobs

import com.example.configs.Config
import com.example.constants.{MetaInfoConstant, MetaInfoReadConstant}
import com.example.readers.RddCsvFileReader
import org.apache.spark.SparkContext

import scala.reflect.io.File

class MetaInfoReadJob(sc: SparkContext) extends ReadJob[Array[Int]] {

  override def read(): Array[Int] = {

    val targetFolder = Config.get(Config.mainFolderKey)

    val (lastPeriodAnalyzed: Int,
      firstMonth: Int,
      firstYear: Int,
      lastMonth: Int,
      lastYear: Int) = {

      val fileFullPath = s"${targetFolder}${MetaInfoConstant.fileName}"

      if (File(fileFullPath).exists) {

        val metaConfig = RddCsvFileReader.Config(

          fileFullPath,
          MetaInfoReadConstant.separator,
          MetaInfoReadConstant.minPartitions,
          MetaInfoReadConstant.headerField,
          MetaInfoReadConstant.headerPos
        )

        val metaInfoReader = new RddCsvFileReader(sc, metaConfig)

        val ((lY, lM), fY, fM) = metaInfoReader
          .read()
          .map(row => {

            val castToInt = (str: String) => str.replaceAll("\"", "").toInt
            (
              (
                castToInt(row(MetaInfoReadConstant.lastYearColPos)),
                castToInt(row(MetaInfoReadConstant.lastMonthColPos))
              ),
              castToInt(row(MetaInfoReadConstant.firstYearColPos)),
              castToInt(row(MetaInfoReadConstant.firstMonthColPos))
            )
          })
          .sortBy(_._1, ascending = false)
          .first()

        (lY*100 + lM, fM, fY, lM, lY)
      }
      else
        {
          (0, 0, 0, 0, 0)
        }
    }
    Array(lastPeriodAnalyzed, firstMonth, firstYear, lastMonth, lastYear)
  }
}
