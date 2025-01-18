package com.example.constants

import com.example.configs.Config

import java.time.format.TextStyle
import java.util.Locale

object WeekDaysForArrivalsInTimeConstant {

  val sortType: Long = SortTypeConstant.sortDesc

  import java.time.DayOfWeek

  def getNameOfWeekDay = (day: Int) => DayOfWeek
    .of(day)
    .getDisplayName(TextStyle.FULL_STANDALONE,
      Locale.forLanguageTag(Config.languageForDisplay))
}