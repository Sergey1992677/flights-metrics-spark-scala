package com.example.writers

import com.opencsv.CSVWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, SaveMode, SparkSession}

import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.reflect.io.File
import scala.util.{Failure, Try}

class AnalyserFileWriter(spark: SparkSession) {

  def writeCsvFile(fileName: String,
                   header: List[String],
                   inRows: List[List[String]],
                   isAppended: Boolean
                  ): Try[Unit] = {

    val rows = if (File(fileName).exists) inRows else header +: inRows

    Try(new CSVWriter(new BufferedWriter(new FileWriter(fileName, isAppended))))
      .flatMap((csvWriter: CSVWriter) =>
        Try {
          csvWriter.writeAll(
            rows.map(_.toArray).asJava
          )
          csvWriter.close()
        } match {
          case f@Failure(_) =>

            Try(csvWriter.close()).recoverWith {
              case _ => f
            }
          case success =>
            success
        }
      )
  }

  def writeDfToParquet(df: DataFrame, filePath: String): Unit = {

    df
      .write
      .mode(SaveMode.Overwrite)
      .save(filePath)
  }

  import spark.implicits._

  def writeRddToParquet[T: Encoder](rdd: RDD[T],
                                    dfSchema: Array[String],
                                    historicalDataPath: String): Unit = {

    val df = rdd.toDF(dfSchema: _*)

    writeDfToParquet(df, historicalDataPath)

    val _ = rdd.unpersist()
  }
}