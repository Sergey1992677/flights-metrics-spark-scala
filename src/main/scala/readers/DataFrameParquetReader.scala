package com.example.readers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class DataFrameParquetReader(spark: SparkSession) extends DataFrameReader{

  override def read(filePath: String, fileSchema: String): DataFrame = {

    spark
      .read
      .schema(fileSchema)
      .parquet(filePath)
  }
}