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

  def getYearlySum: RDD[(String, String)] = {
    salesDataExtracted.groupBy(yearAndId => (yearAndId._1, yearAndId._4)).map(yearAndIdData =>
      (yearAndIdData._1._2, yearAndIdData._1._1, "#", "#",
        yearAndIdData._2.foldLeft(0.toLong)((salesSum, data) => salesSum + data._5))).map(data => (data._1, s"#${data._2}#${data._3}#${data._4}#${data._5}"))

  }

  def getMonthlySum: RDD[(String, String)] = {
    salesDataExtracted.groupBy(monthAndId => (monthAndId._1, monthAndId._2, monthAndId._4)).map(monthAndIdData =>
      (monthAndIdData._1._3, monthAndIdData._1._1, monthAndIdData._1._2, "#",
        monthAndIdData._2.foldLeft(0.toLong)((salesSum, data) => salesSum + data._5))).map(data => (data._1, s"#${data._2}#${data._3}#${data._4}#${data._5}"))

  }

  private def salesDataExtracted: RDD[(Int, Int, Int, String, Long)] = {
    rddSales.map {
      data => {
        val date = new Date(data(0).toLong * 1000L)
        (date.getYear, date.getMonth, date.getDay, data(1), data(2).toLong)
      }
    }
  }

  def getAllSum: RDD[(String, String)] = {
    salesDataExtracted.groupBy(dateAndId => (dateAndId._1, dateAndId._2, dateAndId._3, dateAndId._4)).map(dateAndIdData =>
      (dateAndIdData._1._4, dateAndIdData._1._1, dateAndIdData._1._2, dateAndIdData._1._3,
        dateAndIdData._2.foldLeft(0.toLong)((salesSum, data) => salesSum + data._5))).map(data => (data._1, s"#${data._2}#${data._3}#${data._4}#${data._5}"))

  }

 
}