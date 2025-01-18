package com.example.jobs

import org.apache.spark.SparkContext
import com.example.configs.Config
import com.example.constants.{AirportsReadConstant, FileConstant}
import com.example.readers.RddCsvFileReader
import com.example.transformers.AirDictsTransformer

class AirportsReadJob(sc: SparkContext) extends ReadJob[scala.collection.Map[String, String]] {

  override def read(): scala.collection.Map[String, String] = {

    val targetFolder = Config.get(Config.mainFolderKey)

    val airportConfig = RddCsvFileReader.Config(

      s"${targetFolder}${FileConstant.airDictsFolder}${FileConstant.airportsFile}",
      AirportsReadConstant.separator,
      AirportsReadConstant.minPartitions,
      AirportsReadConstant.headerField,
      AirportsReadConstant.headerPos
    )

    new RddCsvFileReader(sc, airportConfig)
      .read()
      .map(AirDictsTransformer.makeTupleFromArray)
      .collectAsMap()
  }
}
