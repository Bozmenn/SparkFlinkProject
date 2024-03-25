package com.berkozmen

import utils.SparkCtx.SPARK

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.Assert.assertEquals
import org.junit.Test

class MainProcessTest {

  val MOCK_INPUT: DataFrame = {
    val ordersDf = SPARK.read.json(FilePath.ORDERS_PATH)
    val productsDf = SPARK.read.json(FilePath.PRODUCTS_PATH).withColumnRenamed("productid", "product_id")
    productsDf.join(ordersDf, Seq("product_id"), "left")
  }

  @Test
  def calcProductBasePriceChangesTest(): Unit = {
    val resultDf = MainProcess.calcProductBasePriceChanges(MOCK_INPUT)
    val expectedDf = createProductBasePriceChangesDf()
    val diffCount = resultDf.exceptAll(expectedDf).count()
    assertEquals(0, diffCount)
  }

  @Test
  def calcSalesInfoDfTest(): Unit = {
    val resultDf = MainProcess.calcSalesInfo(MOCK_INPUT)
    val expectedDf = createSalesInfoExpectedDf()
    val diffCount = resultDf.exceptAll(expectedDf).count()
    assertEquals(0, diffCount)
  }

  @Test
  def calcAvgSalesInfoForLast5DaysTest(): Unit = {
    val resultDf = MainProcess.calcAvgSalesInfoForLast5Days(MOCK_INPUT)
    val expectedDf = createAvgSalesInfoForLast5DaysDf()
    val diffCount = resultDf.exceptAll(expectedDf).count()
    assertEquals(0, diffCount)
  }

  @Test
  def calcTopSellingLocation(): Unit = {
    val resultDf = MainProcess.calcTopSellingLocation(MOCK_INPUT)
    val expectedDf = createTopSellingLocationDf()
    val diffCount = resultDf.exceptAll(expectedDf).count()
    assertEquals(0, diffCount)
  }

  def createSalesInfoExpectedDf(): DataFrame = {
    val schema = StructType(Seq(
      StructField("product_id", IntegerType, nullable = false),
      StructField("net_sales_amount", IntegerType, nullable = false),
      StructField("net_sales_price", DoubleType, nullable = false),
      StructField("gross_sales_amount", IntegerType, nullable = false),
      StructField("gross_sales_price", DoubleType, nullable = false)
    ))

    val rows = Seq(
      Row(1, 1, 148.0, 5, 298.98),
      Row(2, 1, 1114.0, 5, 1730.0)
    )

    SPARK.createDataFrame(SPARK.sparkContext.parallelize(rows), schema)
  }

  def createAvgSalesInfoForLast5DaysDf(): DataFrame = {
    val schema = StructType(Seq(
      StructField("product_id", IntegerType, nullable = false),
      StructField("avarage_sales_amount_of_last_5_selling_days", DoubleType, nullable = false)
    ))

    val rows = Seq(
      Row(1, 1.5)
    )

    SPARK.createDataFrame(SPARK.sparkContext.parallelize(rows), schema)
  }

  def createTopSellingLocationDf(): DataFrame = {
    val schema = StructType(Seq(
      StructField("product_id", IntegerType, nullable = false),
      StructField("top_selling_location", StringType, nullable = false)
    ))

    val rows = Seq(
      Row(1, "İstanbul"),
      Row(2, "Antalya")
    )

    SPARK.createDataFrame(SPARK.sparkContext.parallelize(rows), schema)
  }

  def createProductBasePriceChangesDf(): DataFrame = {
    val schema = StructType(Seq(
      StructField("product_id", IntegerType, nullable = false),
      StructField("price", DoubleType, nullable = false),
      StructField("order_date", TimestampType, nullable = false),
      StructField("change", StringType, nullable = false)
    ))

    val data = Seq(
      Row(1, 35.99, java.sql.Timestamp.valueOf("2021-05-10 00:00:02"), "fall"),
      Row(1, 39.5, java.sql.Timestamp.valueOf("2021-05-08 00:02:25"), "fall"),
      Row(1, 39.5, java.sql.Timestamp.valueOf("2021-05-08 00:01:25"), "fall"),
      Row(1, 149.0, java.sql.Timestamp.valueOf("2020-01-01 00:02:48"), "fall"),
      Row(1, 34.99, java.sql.Timestamp.valueOf("2020-01-01 00:01:39"), "rise"),
      Row(2, 179.0, java.sql.Timestamp.valueOf("2021-01-01 00:06:40"), "fall"),
      Row(2, 19.0, java.sql.Timestamp.valueOf("2021-01-01 00:05:02"), "rise"),
      Row(2, 289.0, java.sql.Timestamp.valueOf("2021-01-01 00:03:12"), "fall"),
      Row(2, 399.0, java.sql.Timestamp.valueOf("2021-01-01 00:03:04"), "fall"),
      Row(2, 844.0, java.sql.Timestamp.valueOf("2021-01-01 00:02:56"), "fall")
    )

    SPARK.createDataFrame(SPARK.sparkContext.parallelize(data), schema)
  }
}
