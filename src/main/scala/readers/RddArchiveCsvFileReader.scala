package com.example.readers

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream
import java.nio.file.{FileSystems, Files}
import scala.collection.JavaConverters._

object RddArchiveCsvFileReader extends RddReaderUtil {

  case class Config(filePath: String,
                    separator: Char,
                    minPartitions: Int,
                    headerField: String,
                    headerPos: Byte)
}

class RddArchiveCsvFileReader(sc: SparkContext,
                              config: RddArchiveCsvFileReader.Config) extends RddReader[Array[String]] {

  override def read(): RDD[Array[String]] = {

    val tmpConfig = config

    val tmpGetRecord = RddCsvFileReader.getRecord(_,
      tmpConfig.separator, tmpConfig.headerPos, tmpConfig.headerField)

    sc.binaryFiles(tmpConfig.filePath, tmpConfig.minPartitions)
      .flatMap { case (name: String, content: PortableDataStream) =>

        val zis = new ZipInputStream(content.open)

        Stream.continually(zis.getNextEntry)
          .takeWhile(_ != null)
          .flatMap { _ =>

            val br = new BufferedReader(new InputStreamReader(zis))

            Stream
              .continually(br.readLine())
              .takeWhile(_ != null)
          }
      }
      .mapPartitions(tmpGetRecord)
  }
}

class MultipleRddArchiveCsvFileReader(sc: SparkContext,
                                      config: RddArchiveCsvFileReader.Config,
                                      folderWithArchives: String) extends RddReader[Array[String]] {

  override def read(): RDD[Array[String]] = {

    val tmpConfig = config

    val rddList = List[RDD[Array[String]]]()

    val dir = FileSystems
      .getDefault
      .getPath(folderWithArchives)

    Files
      .list(dir)
      .iterator()
      .asScala
      .map(file =>
        {
          val fileConfig = RddArchiveCsvFileReader.Config(
            file.toString,
            tmpConfig.separator,
            tmpConfig.minPartitions,
            tmpConfig.headerField,
            tmpConfig.headerPos
          )

          val reader = new RddArchiveCsvFileReader(sc, fileConfig)

          reader.read()
        }
      )
      .reduceLeft((a, b) => a.union(b))
  }
}