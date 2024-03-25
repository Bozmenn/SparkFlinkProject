package com.berkozmen
package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameWriter {
  def write(df: DataFrame, fileFormat: String,
                      mode: String,
                      outputPath: String): Unit = {
    df.write
      .format(fileFormat)
      .mode(mode)
      .option("nullValue", 0)
      .save(outputPath)
  }
}
