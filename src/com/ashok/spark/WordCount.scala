package com.ashok.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by ashok on 8/25/2018.
  */
object WordCount extends App{

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "WordCountSorted")

  // Read each line of input data
  val lines = sc.textFile("../sparklectures/SparkScala/book.txt")

  //Use regular expressions to split cleanly
  //val words = lines.flatMap(x => x.split("\\W+"))
  val words = lines.map(x => x.split("\\W+"))
  val words1 = words.collect()

  for(wcs <- words){
    println(wcs)
  }


  //words1.foreach(println)

  //Convert to lowerCase
  //val lowerWords = words.map(x => x.toLowerCase())

  //val wc = lowerWords.countByValue()
  //val wc = lowerWords.map(x => (x,1)).reduceByKey((x,y) => x + y)

  //val finalWC = wc.map(x => (x._2, x._1 )).sortByKey()


 /* for(wcs <- finalWC){
    val count = wcs._1
    val word = wcs._2
    println(s"$word: $count")
  }*/

  //finalWC.foreach(println)

}
