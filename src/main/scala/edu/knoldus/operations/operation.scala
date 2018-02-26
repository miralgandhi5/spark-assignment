package edu.knoldus.operations

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object operation {
  val sc = new SparkContext("local", "operations")

  val rddCustomer: RDD[Array[String]] = sc.textFile("/home/knoldus/IdeaProjects/spark-assignment/customerData")
    .map { data => data.split('#') }
  val rddSales: RDD[Array[String]] = sc.textFile("/home/knoldus/IdeaProjects/spark-assignment/salesData")
    .map { data => data.split('#') }

  def customerDataExtracted: RDD[(String, String)] = rddCustomer.map {
    data => {
      (data(0), data(3))
    }
  }

  private def salesDataExtracted: RDD[(Int, Int, Int, String, Long)] = {
    rddSales.map {
      data => {
        val date = new Date(data(0).toLong * 1000L)
        (date.getYear, date.getMonth, date.getDay, data(1), data(2).toLong)
      }
    }
  }

  def getYearlySum: RDD[(String, String)] = {
    salesDataExtracted.groupBy(yearAndId => (yearAndId._1, yearAndId._4)).map(yearAndIdData =>
      (yearAndIdData._1._2, yearAndIdData._1._1, "#", "#",
        yearAndIdData._2.foldLeft(0.toLong)((accumulate, tuple) => accumulate + tuple._5))).map(data => (data._1, s"#${data._2}#${data._3}#${data._4}#${data._5}"))

}
