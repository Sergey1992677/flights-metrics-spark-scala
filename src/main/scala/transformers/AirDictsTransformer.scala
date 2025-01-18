package com.example.transformers

import org.apache.log4j.LogManager

object AirDictsTransformer {

  def makeTupleFromArray(inputArray: Array[String]): (String, String) = (inputArray(0), inputArray(1))

  def getNameFromDict(key: String, dict: scala.collection.Map[String, String]): String = {

    dict.getOrElse(key, {

      val log = LogManager.getRootLogger

      val errorMsg = s"Ошибка: кода (${key}) нет в справочнике"

      log.error(errorMsg)

      errorMsg
    })
  }
}