package com.example.schemas

object WritersSchemas {

  val metaInfo = List("collected", "processed", "firstMonth", "firstYear", "lastMonth", "lastYear")

  val topAirports = Array("Airport", "Flights")

  val topAirlinesInTime = Array("Airline", "Flights")

  val topAirlinesByAirport = Array("Airport", "Airline")

  val topAirportsDestByAirport = Array("AirportOrigin", "AirportDest")

  val weekDaysForArrivalsInTime = Array("dayOfWeek", "Flights")

  val delayedFlightsReasons = Array("AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY")
}