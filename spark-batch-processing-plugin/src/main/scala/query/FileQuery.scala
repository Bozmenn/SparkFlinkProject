package com.berkozmen
package query

import com.berkozmen.constant.ColNames
import com.berkozmen.utils.Constants.LEFT
import com.berkozmen.utils.{Constants, FilePath}
import com.berkozmen.utils.SparkCtx.SPARK
import org.apache.spark.sql.DataFrame

object FileQuery extends ColNames{

  lazy val INPUT_DF: DataFrame = {
    val ordersDf = SPARK.read.json(FilePath.ORDERS_PATH)
    val productsDf = SPARK.read.json(FilePath.PRODUCTS_PATH)
      .withColumnRenamed("productid", PROD_ID)

    productsDf.join(ordersDf, Seq(PROD_ID), LEFT)
  }

}
