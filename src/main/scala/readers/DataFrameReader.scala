package com.example.readers

import org.apache.spark.sql.DataFrame

trait DataFrameReader {

  def read(filePath: String, fileSchema: String): DataFrame
}