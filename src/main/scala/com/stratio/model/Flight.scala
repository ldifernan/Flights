package com.stratio.model

import com.stratio.utils.ParserUtils
import org.joda.time.DateTime

sealed case class Cancelled (id: String) {override def toString: String = id}

object OnTime extends Cancelled (id ="OnTime")
object Cancel extends Cancelled (id ="Cancel")
object Unknown extends Cancelled (id ="Unknown")

case class Delays (
                    carrier: Cancelled,
                    weather: Cancelled,
                    nAS: Cancelled,
                    security: Cancelled,
                    lateAircraft: Cancelled)

case class Flight (date: DateTime, //Tip: Use ParserUtils.getDateTime

                   departureTime: Int,
                   crsDepatureTime: Int,
                   arrTime: Int,
                   cRSArrTime: Int,
                   uniqueCarrier: String,
                   flightNum: Int,
                   actualElapsedTime: Int,
                   cRSElapsedTime: Int,
                   arrDelay: Int,
                   depDelay: Int,
                   origin: String,
                   dest: String,
                   distance: Int,
                   cancelled: Cancelled,
                   cancellationCode: Int,
                   delay: Delays)


  object Flight {
    /*
    *
    * Create a new Flight Class from a CSV file
    *
    */
    def apply(fields: Array[String]): Flight = {
      val (firstChunk, secondChunk) = fields.splitAt(22)
      val Array(year, month, dayOfMonth, dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime, uniqueCarrier,
      flightNum, _, actualElapsedTime, cRSElapsedTime, _, arrDelay, depDelay, origin, dest, distance, _, _, cancelled) = firstChunk

      val Array(cancellationCode, diverted, carrierDelay, weatherDelay, nASDelay, securityDelay, lateAircraftDelay) = secondChunk

      Flight(
        ParserUtils.getDateTime(year.toInt, month.toInt, dayOfMonth.toInt),
        departureTime.toInt,
        crsDepatureTime.toInt,
        arrTime.toInt,
        cRSArrTime.toInt,
        uniqueCarrier.toString,
        flightNum.toInt,
        actualElapsedTime.toInt,
        cRSElapsedTime.toInt,
        arrDelay.toInt,
        depDelay.toInt,
        origin.toString,
        dest.toString,
        distance.toInt,
        parseCancelled(cancelled),
        cancellationCode.toInt,
        Delays(parseCancelled(carrierDelay), parseCancelled(weatherDelay), parseCancelled(nASDelay),
          parseCancelled(securityDelay),parseCancelled(lateAircraftDelay)))
    }

    /*
     *
     * Extract the different types of errors in a string list
     *
     */
    def extractErrors(fields: Array[String]): Seq[String] = {
      val (firstChunk, secondChunk) = fields.splitAt(22)
      val Array(year, month, dayOfMonth, dayOfWeek, departureTime, crsDepatureTime, arrTime, cRSArrTime, uniqueCarrier,
      flightNum, _, actualElapsedTime, cRSElapsedTime, airTime, arrDelay, depDelay, origin, dest, distance, _, _, cancelled) = firstChunk

      val Array(cancellationCode, diverted, carrierDelay, weatherDelay, nASDelay, securityDelay, lateAircraftDelay) = secondChunk

      val errorsInt = Seq(year,month, dayOfMonth, departureTime, crsDepatureTime, arrTime, cRSArrTime, flightNum,
        actualElapsedTime, cRSElapsedTime, airTime, arrDelay, depDelay, distance,cancellationCode).flatMap(evaluarInt(_))
      val errorsEnum = evaluarEnum(cancelled)

      errorsInt++errorsEnum
    }
    def evaluarInt(elemento : String): Option[(String)] = {
      if(evalInt(elemento)==1) Some(("Error Tipo 1"))
      else if (evalInt(elemento)==2) Some(("Error Tipo 2"))
      else None
    }

    def evalInt(in: String): Int = {
      if(in == "NA") 2
      else
        try {
          Integer.parseInt(in.trim)
          0
        } catch {
          case e: NumberFormatException => 1
        }
    }


    def evaluarEnum(elemento : String): Option[(String)] = {
      if(evalEnum(elemento)==1) Some(("Error Tipo 1"))
      else if (evalEnum(elemento)==2) Some(("Error Tipo 2"))
      else None
    }
    def evalEnum(in: String): Int = {
      if(in == "NA") 2
      else
        try {
          Integer.parseInt(in.trim)
          if (in.toInt==0 || in.toInt==1) 0
          else 1
        } catch {
          case e: NumberFormatException => 1
        }
    }



    /*
    *
    * Parse String to Cancelled Enum:
    *   if field == 1 -> Cancel
    *   if field == 0 -> OnTime
    *   if field <> 0 && field<>1 -> Unknown
    */
    def parseCancelled(field: String): Cancelled = {
      try {
        Integer.parseInt(field.trim)
        if (field.toInt == 0) OnTime
        else if (field.toInt == 1) Cancel
        else Unknown
      }
      catch {
        case e: NumberFormatException => Unknown
      }
    }
  }

