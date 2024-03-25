package com.berkozmen
package utils

object FilePath {

  val PATH = System.getProperty("user.dir")

  val ORDERS_PATH = PATH + "/data/orders.json"
  val PRODUCTS_PATH = PATH + "/data/products.json"
  val FIRST_PROBLEM_OUTPUT_PATH = PATH + "/data/output/firstJobResult"
  val SECOND_PROBLEM_OUTPUT_PATH = PATH + "/data/output/secondJobResult"

}
