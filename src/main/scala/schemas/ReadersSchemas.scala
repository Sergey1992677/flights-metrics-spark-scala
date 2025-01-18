package com.example.schemas

object ReadersSchemas {

  val topAirports = "Airport STRING, Flights LONG"

  val topAirlinesInTime = "Airline STRING, Flights LONG"

  val topAirlinesByAirport = "Airport STRING, Airline STRING"

  val topAirportsDestByAirport = "AirportOrigin STRING, AirportDest STRING"

  val weekDaysForArrivalsInTime = "dayOfWeek STRING, Flights LONG"

  val delayedFlightsReasons = "AIR_SYSTEM_DELAY LONG, SECURITY_DELAY LONG, AIRLINE_DELAY LONG, LATE_AIRCRAFT_DELAY LONG, WEATHER_DELAY LONG"
}