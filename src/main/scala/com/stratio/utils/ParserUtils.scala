package com.stratio.utils

import org.joda.time.DateTime

import scala.util.{Failure, Try, Success}

object ParserUtils {

  def getDateTime(year: Int, month: Int, day: Int): DateTime =
    new DateTime().year().setCopy(year).monthOfYear().setCopy(month).dayOfMonth().setCopy(day)
}