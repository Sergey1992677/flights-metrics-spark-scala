package com.example.jobs

import com.example.configs.Config
import com.example.constants.{FileConstant, FlightsReadConstant}
import com.example.readers.{MultipleRddArchiveCsvFileReader, RddArchiveCsvFileReader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class FlightsReadJob(sc: SparkContext) extends ReadJob[RDD[Array[String]]] {

  override def read(): RDD[Array[String]] = {

    val targetFolder = Config.get(Config.mainFolderKey)

    val flightsConfig = RddArchiveCsvFileReader.Config(
      //  при чтении множетсва архивов путь к одному архиву не нужен
      "",
      FlightsReadConstant.separator,
      FlightsReadConstant.minPartitions,
      FlightsReadConstant.headerField,
      FlightsReadConstant.headerPos
    )

    new MultipleRddArchiveCsvFileReader(
      sc,
      flightsConfig,
      s"${targetFolder}${FileConstant.flightsFolder}")
      .read()
  }
}
