package com.example.readers

import org.apache.spark.rdd.RDD

trait RddReader[T] {

  def read(): RDD[T]
}