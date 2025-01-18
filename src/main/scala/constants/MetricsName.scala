package com.example.constants

object MetricsName {

  def getTopAirports(top: Int, sortOrd: Long): String = {

    val sortMap = Map(1L -> "непопулярных", -1L -> "популярных")

    s"Топ-${top} самых ${sortMap(sortOrd)} аэропортов"
  }

  def getTopAirlines(top: Int, sortOrd: Long): String = {

    val sortMap = Map(1L -> "худших", -1L -> "лучших")

    s"Топ-${top} ${sortMap(sortOrd)} авиакомпаний, вовремя выполняющих рейсы"
  }

  def getTopAirlinesByAirport(top: Long, sortOrd: Long): String = {

    val sortMap = Map(1L -> "мелких", -1L -> "крупных")

    s"Топ-${top} самых ${sortMap(sortOrd)} перевозчиков для аэропорта по вылетам вовремя"
  }

  def getTopAirportsDestByAirport(top: Long, sortOrd: Long): String = {

    val sortMap = Map(1L -> "мелких", -1L -> "крупных")

    s"Топ-${top} самых ${sortMap(sortOrd)} аэропортов назначения для аэропорта вылета по вылетам вовремя"
  }

  def getWeekDaysForArrivalsInTime(sortOrd: Long): String = {

    val sortMap = Map(1L -> "несвоевременности", -1L -> "своевременности")

    s"Дни недели в порядке ${sortMap(sortOrd)} прибытия рейсов"
  }

  val delayedReasonsMetric = "Сколько рейсов были на самом деле задержаны по причине"

  val delayedReasonsPercentMetric = "Процент от общего количества минут задержки рейсов"

}
