package com.example.jobs

import com.example.configs.Config
import com.example.constants.{AirlinesReadConstant, FileConstant}
import com.example.readers.RddCsvFileReader
import com.example.transformers.AirDictsTransformer
import org.apache.spark.SparkContext

class AirlinesReadJob(sc: SparkContext) extends ReadJob[scala.collection.Map[String, String]] {

  override def read(): scala.collection.Map[String, String] = {

    val targetFolder = Config.get(Config.mainFolderKey)

    val airlinesConfig = RddCsvFileReader.Config(

      s"${targetFolder}${FileConstant.airDictsFolder}${FileConstant.airlinesFile}",
      AirlinesReadConstant.separator,
      AirlinesReadConstant.minPartitions,
      AirlinesReadConstant.headerField,
      AirlinesReadConstant.headerPos
    )

    new RddCsvFileReader(sc, airlinesConfig)
      .read()
      .map(AirDictsTransformer.makeTupleFromArray)
      .collectAsMap()
  }
}
