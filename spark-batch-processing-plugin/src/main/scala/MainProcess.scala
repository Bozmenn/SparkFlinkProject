package com.berkozmen

import constant.ColNames

import com.berkozmen.query.FileQuery
import com.berkozmen.utils.Constants.LEFT
import com.berkozmen.utils.SparkCtx.SPARK
import com.berkozmen.utils.{DataFrameWriter, FilePath}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, max, rank, sum, to_date, to_timestamp, when}

object MainProcess extends ColNames {

  private val ORDER_COUNT = "order_count"
  private val RANK = "rank"
  private val TOP_SELLING_LOCATION = "top_selling_location"
  private val CHANGE = "change"
  private val LAG_PRICE = "lag_price"

  def main(args: Array[String]): Unit = {

    val inputDf = FileQuery.INPUT_DF
    val salesInfoDf = calcSalesInfo(inputDf)
    val avgSalesInfoForLast5DaysDf = calcAvgSalesInfoForLast5Days(inputDf)
    val topSellingLocationDf = calcTopSellingLocation(inputDf)

    val firstProblemResultDf = salesInfoDf
      .join(avgSalesInfoForLast5DaysDf, Seq(PROD_ID), LEFT)
      .join(topSellingLocationDf, Seq(PROD_ID), LEFT)

    val secondProblemResultDf = calcProductBasePriceChanges(inputDf)

    DataFrameWriter.write(firstProblemResultDf,
      "json",
      "overwrite",
      FilePath.FIRST_PROBLEM_OUTPUT_PATH)

    DataFrameWriter.write(secondProblemResultDf,
      "json",
      "overwrite",
      FilePath.SECOND_PROBLEM_OUTPUT_PATH)

    SPARK.stop()
  }

  def calcProductBasePriceChanges(df: DataFrame): DataFrame = {
    val selectedCols = Seq(PROD_ID, PRICE, ORDER_DATE, CHANGE).map(col)
    val dfWithTime = df.withColumn(ORDER_DATE, to_timestamp(col(ORDER_DATE)))
    val windowSpec = Window.partitionBy(PROD_ID).orderBy(col(ORDER_DATE).desc)
    val dfWithLagPrice = dfWithTime.withColumn(LAG_PRICE,
      lag(PRICE, 1).over(windowSpec))
    dfWithLagPrice.withColumn(CHANGE, when(col(PRICE) <
      col(LAG_PRICE), "rise").otherwise("fall"))
      .select(selectedCols: _*)
  }

  def calcTopSellingLocation(df: DataFrame): DataFrame = {
    val orderCountsDf = df.groupBy(PROD_ID, LOCATION)
      .agg(count(ORDER_ID).as(ORDER_COUNT))
    val windowSpec = Window.partitionBy(PROD_ID)
      .orderBy(col(ORDER_COUNT).desc)
    val rankedLocations = orderCountsDf.withColumn(RANK, rank().over(windowSpec))
    rankedLocations.filter(col(RANK) === 1)
      .select(col(PROD_ID),
        col(LOCATION).as(TOP_SELLING_LOCATION))
  }

  def calcAvgSalesInfoForLast5Days(df: DataFrame): DataFrame = {
    val filteredDf = filterForLast5SoldDaysRecord(df)
    val dailySalesDf = filteredDf.groupBy(PROD_ID, ORDER_DATE).agg(count(ORDER_ID)
      .as("sales_amount_of_day"))
    val result = dailySalesDf.groupBy(PROD_ID).agg(avg("sales_amount_of_day")
      .as("avarage_sales_amount_of_last_5_selling_days"))
    result
  }

  def filterForLast5SoldDaysRecord(df: DataFrame): DataFrame = {
    val dfWithDate = df.select(PROD_ID, ORDER_DATE, ORDER_ID)
      .withColumn(ORDER_DATE, to_date(to_timestamp(col(ORDER_DATE),
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))
    val lastSoldDay = dfWithDate.agg(max(col(ORDER_DATE)).as("last_sold_day"))
      .select("last_sold_day").first().toString()
    val trimmedLastSoldDay = lastSoldDay.substring(1, lastSoldDay.length() - 1)
    val minusFiveDaysDate = java.time.LocalDate.parse(trimmedLastSoldDay).minusDays(5).toString()
    //Automatically compare string and date
    dfWithDate.filter(col(ORDER_DATE) >= minusFiveDaysDate)

  }

  def calcSalesInfo(df: DataFrame): DataFrame = {
    val aggList = List(
      ("net_sales_amount", count(when(col(STATUS) === "Created", col(ORDER_ID))) -
        count(when(col(STATUS) === "Cancelled"
          or col(STATUS) === "Returned", col(ORDER_ID)))),
      ("net_sales_price", sum(when(col(STATUS) === "Created", col(PRICE)).otherwise(0)) -
        sum(when(col(STATUS) === "Cancelled"
          or col(STATUS) === "Returned", col(PRICE)).otherwise(0))),
      ("gross_sales_amount", count(ORDER_ID)),
      ("gross_sales_price", sum(PRICE)),
    ).map(c => c._2.as(c._1))

    df.groupBy(PROD_ID).agg(aggList.head, aggList.tail: _*)
  }
}
