package com.ashok.spark

import org.apache.log4j._
import org.apache.spark._

/**
  * Created by ashok on 8/25/2018.
  */
object FriendsByAge extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line:String) = {
    val fields = line.split(",")
    //val age = fields(2).toInt
    val numFriends = fields(3).toInt
    val fName = fields(1)
    (fName,numFriends)
  }
  val sc = new SparkContext("local[*]", "FriendsByAge")
  val lines = sc.textFile("../sparklectures/SparkScala/fakefriends.csv")
  val rdd = lines.map(parseLine)
  //(ashok,(333,1))
  val rddValues = rdd.mapValues(x=> (x,1)).reduceByKey((x,y)=> (x._1 + y._1, x._2+y._2)).mapValues(x => x._1/x._2)
  //val rddValues = rdd.mapValues(x=> (x,1)).reduceByKey((x,y)=> (x._1 + y._1,x._2+y._2)).mapValues(x => (x._1/x._2))
  //(33,(330,1))
  //(33,(340,1))
  //(33,(670,2))
  //val reduceByKeyRDD = rddValues.reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2))
  //val avergaeRDD = reduceByKeyRDD.mapValues(x => x._1/x._2)

  val results = rddValues.collect()

  results.sorted.foreach(println)

}
