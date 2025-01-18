package com.example.transformers

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RankCalcTransformer {

  private def rank(iter: Iterator[((Long, String, String), Int)]) = {

    val zero = List((0L, (Long.MinValue, "", ""), 0, 1L))

    def f(acc: List[(Long, (Long, String, String), Int, Long)], x: ((Long, String, String), Int)) =

      (acc.head, x) match {

        case (

          (prevRank: Long, (prevValue: Long, _, _), _, offset: Long),

          ((currValue: Long, colOne: String, colTwo: String), _)) => {

          val newRank = if (prevValue == currValue) prevRank else prevRank + offset

          val newOffset = if (prevValue == currValue) offset + 1L else 1L

          (newRank, (currValue, colOne, colTwo), 0, newOffset) :: acc
        }
      }

    iter.foldLeft(zero)(f).reverse.drop(1).map { case (rank, label, _, _) =>
      (rank, label)
    }.iterator
  }

  //  создаю свой разделитель по партициям
  import org.apache.spark.Partitioner

  private class AirportPartitioner(override val numPartitions: Int,
                                   airportsMap: scala.collection.Map[String, Int]) extends Partitioner {

    def getPartition(key: Any): Int = key match {
      case tpl: (Int, String, String) @unchecked => airportsMap.getOrElse(tpl._2, 0)
    }
  }

  def calcRank(rdd: RDD[((Long, String, String), Int)], sc: SparkContext): RDD[(Long, (Long, String, String))] = {

    val airportsMap = rdd
      .map(row => (row._1._2, 0))
      .reduceByKey(_ + _)
      .collectAsMap()
      .zipWithIndex
      .map(el => (el._1._1 -> el._2))

    val partitioner = new AirportPartitioner(airportsMap.size, airportsMap)

    rdd
      //  получаем отсортированные строки внутри партиций
      .repartitionAndSortWithinPartitions(partitioner)
      //  считаем ранги для партиций
      .mapPartitions(rank)
  }

  def aggForRank(rdd: RDD[Array[String]],
                 colOnePos: Byte,
                 colTwoPos: Byte,
                 sortType: Long) = {

    rdd
      .map(
        row => (
          (
            row(colOnePos), row(colTwoPos)
          ),
          sortType * 1L
        ))
      .reduceByKey(_ + _)
      //  0 использую ниже для сохранения структуры ключ - значение, на ранг не влияет
      .map(row => ((row._2, row._1._1, row._1._2), 0))
  }
}
