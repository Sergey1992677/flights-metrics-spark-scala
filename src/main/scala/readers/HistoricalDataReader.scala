package com.example.readers

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, Encoders, Encoder}

import scala.reflect.ClassTag

import java.nio.file.{Files, Paths}

case class HistoricalDataReader[A:ClassTag](spark: SparkSession,
                                   sc: SparkContext,
                                   filePath: String,
                                   dfSchema: String,
                                   funcTransformer: Row => A
                                  ) {

  def read(): RDD[A] = {

    if (Files.exists(Paths.get(filePath))) {

      implicit def kryoEncoder: Encoder[A] = Encoders.kryo[A]

      val reader = new DataFrameParquetReader(spark)

      reader
        .read(filePath, dfSchema)
        .map(funcTransformer)
        .rdd
    }
    else sc.emptyRDD[A]
  }
}

object HistoricalDataReader {

  val makeTuple5FromRow = (row: Row) => (row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4))

  val makeArrayFromRow = (row: Row) => Array(row.getString(0), row.getString(1))

  val makeTuple2FromRow = (row: Row) => (row.getString(0), row.getLong(1))
}