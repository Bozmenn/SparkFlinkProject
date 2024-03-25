package com.berkozmen.utils

import com.berkozmen.Model.Order
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.java.typeutils.TypeExtractor

import java.nio.charset.StandardCharsets

class OrderDeserializationSchema extends DeserializationSchema[Order] {
  @transient lazy val objectMapper = new ObjectMapper()

  override def deserialize(message: Array[Byte]): Order = {
    objectMapper.readValue(new String(message, StandardCharsets.UTF_8), classOf[Order])
  }

  override def isEndOfStream(nextElement: Order): Boolean = false

  override def getProducedType: TypeInformation[Order] = {
    TypeExtractor.getForClass(classOf[Order])
  }
}

