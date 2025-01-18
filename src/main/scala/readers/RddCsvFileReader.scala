package com.example.readers

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RddCsvFileReader extends RddReaderUtil {

  case class Config(filePath: String,
                    separator: Char,
                    minPartitions: Int,
                    headerField: String,
                    headerPos: Byte)
}

class RddCsvFileReader(sc: SparkContext,
                       config: RddCsvFileReader.Config) extends RddReader[Array[String]] {

  override def read(): RDD[Array[String]] = {

    //  проблема сериализации, без локальных переменных будет выдавать
    //  ошибку Task not serializable!
    val tmpConfig = config

    val tmpGetRecord = RddCsvFileReader.getRecord(_,
      tmpConfig.separator, tmpConfig.headerPos, tmpConfig.headerField)

    sc.textFile(tmpConfig.filePath, tmpConfig.minPartitions)
      .mapPartitions(tmpGetRecord)
  }
}