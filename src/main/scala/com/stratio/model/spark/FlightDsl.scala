package com.stratio.model.spark

import scala.language.implicitConversions
import com.stratio.model.Flight
import com.stratio.utils.ParserUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.joda.time.DateTime


class FlightCsvReader(self: RDD[String]) {

  /**
   *
   * Parser the csv file with the format described in the readme.md file to a Fligth class
   *
   */
  def toFlight: RDD[Flight] = {
    errorOrFlight.filter(_.isRight).map(_.right.get)
  }

  /**
   *
   * Obtain the parser errors
   *
   */
  def toErrors: RDD[(String, String)] = {
    errorOrFlight.filter(_.isLeft).map(_.left.get)
  }

  /**
   *
   * Aux function that helps obtaining errors and Flights
   *
   */
  def errorOrFlight: RDD[Either[(String, String), Flight]] = {
    val headers = self.first
    self.filter(_ != headers).flatMap(line => {
      val fields = line.split(",")
      val errorList = Flight.extractErrors(fields)
      if (errorList.isEmpty) {
        Seq(Right(Flight(fields)))
      }
      else {
        errorList.map(error => {
          Left((error, line))
        })
      }
    }
    )
  }
}

  class FlightFunctions(self: RDD[Flight]) {

    /**
     *
     * Obtain the minimum fuel's consumption using a external RDD with the fuel price by Month
     *
     */
    def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[String]): RDD[(String, Short)] = ??? //{
//      val prices = fuelPrice.map(x=>x.split(",")).map(linea=>(linea(0).toInt, linea(1).toInt, linea(2).toInt))
//      val origMonthDist = self.map(linea=>((linea.date.monthOfYear().get(), linea.origin),linea.distance)).reduceByKey(_+_)
//        .map(linea=> ((linea._1._1),(linea._1._2,linea._2,prices.map(_._3))))
//      val result = origMonthDist.map(x=>(x._1,(x._2._1 , x._2._2 * (x._2._3))))
//      result.map(x=>(x._2._1, (x._1,x._2._2))).reduceByKey((x,y)=>if(x._2 < y._2) x else y)
//    }

    /**
     *
     * Obtain the average distance flyed by airport, taking the origin field as the airport to group
     *
     */

    def averageDistanceByAirport: RDD[(String, Float)] = {
      val distances = self.map(linea => (linea.origin,linea.distance.toFloat))
      distances.reduceByKey(_+_).map(linea=>(linea._1,linea._2/self.count()))
    }

    /**
     *
     * Reasign the dest Airport and destHour to the ghost flights being a ghost flight those whom doesn't
     *
     */
    def asignGhostFlights(elapsedSeconds: Int): RDD[Flight] = ???

  }




  trait FlightDsl {

    implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

    implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
  }

  object FlightDsl extends FlightDsl



