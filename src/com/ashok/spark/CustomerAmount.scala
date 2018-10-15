package com.ashok.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by ashok on 8/25/2018.
  */
object CustomerAmount extends App {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using the local machine
  val sc = new SparkContext("local[*]", "CustomerAmount")

  val cusData = sc.textFile("../sparklectures/SparkScala/customer-orders.csv")

  def parseData(line: String) = {
    val data = line.split(",")
    val cuID = data(0).toInt
    val amt = data(2).toFloat
    (cuID,amt)
  }
  val cusDataFmt = cusData.map(parseData).reduceByKey((x,y) => (x + y)).map(x => (x._2,x._1)).sortByKey()

  val finalData = cusDataFmt.collect()

  for (fData <- finalData) {

    val custID = fData._2
    val amtSpent = fData._1
    println(s"$custID spent $amtSpent")

  }


}
