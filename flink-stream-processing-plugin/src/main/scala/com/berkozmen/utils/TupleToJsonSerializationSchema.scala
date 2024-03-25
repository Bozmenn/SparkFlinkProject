package com.berkozmen.utils

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper

import java.sql.Timestamp

class TupleToJsonSerializationSchema extends SerializationSchema[(Long, Long, String, Timestamp)] {
  @transient lazy val objectMapper = new ObjectMapper()

  override def serialize(value: (Long, Long, String, Timestamp)): Array[Byte] = {
    try {
      val resultMap = Map(
        "seller_count" -> value._1,
        "product_count" -> value._2,
        "location" -> value._3,
        "date" -> value._4.getTime
      )
      objectMapper.writeValueAsBytes(resultMap)
    } catch {
      case e: Exception =>
        println("Serialization error: " + e.getMessage)
        null
    }
  }
}

