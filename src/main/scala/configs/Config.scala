package com.example.configs

import com.example.constants.FileConstant

object Config {

  val mainFolderKey = "folder"

  val appMode = "local"

  private val dev: Map[String, String] = {

    Map(appMode -> "local",
      mainFolderKey -> FileConstant.folderLocalMode)
  }

  private val prod: Map[String, String] = {

    Map("master" -> "spark://spark:7077",
      "folder" -> FileConstant.folderClusterMode)
  }

  private val environment = sys.env.getOrElse("PROJECT_ENV", "dev")

  def get(key: String): String = {

    environment match {
      case "dev" => dev(key)
      case _ => prod(key)
    }
  }

  val languageForDisplay = "ru"

  val appName = "FlightAnalyzerApp"
}
