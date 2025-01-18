package com.example.schemas

object MetricsSchemas {

  val topAirports = Array("Показатель", "Аэропорт", "Рейсы")

  val topAirlines = Array("Показатель", "Авиакомпания", "Рейсы")

  val topAirlinesByAirport = Array("Показатель", "Аэропорт", "Перевозчик", "Ранг", "Вылеты")

  val topAirportsDestByAirport = Array("Показатель", "Аэропорт вылета", "Аэропорт назначения", "Ранг", "Вылеты")

  val weekDaysForArrivalsInTime = Array("Показатель", "День недели", "Рейсы")

  val delayedFlightsReasons = Array("Показатель", "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
}