package com.example.constants

import com.example.schemas.WritersSchemas

object MetaInfoReadConstant {

  private val headerMetaColPos: Byte = 0

  val separator: Char = ','

  val minPartitions: Int = 1

  val headerField = s""""${WritersSchemas.metaInfo.apply(headerMetaColPos)}""""

  val headerPos: Byte = headerMetaColPos

  val fakelastPeriodAnalyzed: Int = 0

  val firstMonthColPos: Byte = 2

  val firstYearColPos: Byte = 3

  val lastMonthColPos: Byte = 4

  val lastYearColPos: Byte = 5
}
