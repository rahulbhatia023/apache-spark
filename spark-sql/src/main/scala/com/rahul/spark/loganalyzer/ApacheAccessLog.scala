package com.rahul.spark.loganalyzer

import scala.util.matching.Regex

case class ApacheAccessLog(
                            ipAddress: String,
                            dateTime: String,
                            method: String,
                            endpoint: String,
                            protocol: String,
                            responseCode: Int,
                            contentSize: Long) {
}

object ApacheAccessLog {
  val PATTERN: Regex = "(.*) - - \\[(.*)\\] \\\"(.*) (.*) (.*)\\\" (.*) (.*)".r

  def parseLogLine(log: String): ApacheAccessLog = {
    log match {
      case PATTERN(ipAddress, dateTime, method, endpoint, protocol, responseCode, contentSize)
      => ApacheAccessLog(ipAddress, dateTime, method, endpoint, protocol, responseCode.toInt, contentSize.toLong)
      case _ => throw new RuntimeException("Cannot parse log line: " + log)
    }
  }
}