package com.ashok.spark

import breeze.linalg.{max, min}
import org.apache.log4j._
import org.apache.spark._

/**
  * Created by ashok on 8/25/2018.
  */
object MinMaxTemperatures {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinMaxTemperatures")

    // Read each line of input data
    val lines = sc.textFile("../sparklectures/SparkScala/1800.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMIN entries
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    //val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))
    //val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))

    // Collect, format, and print the results
    val results = maxTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station Maximum temperature: $formattedTemp")
      //println(s"$station minimum temperature: $formattedTemp")
    }

  }

}
