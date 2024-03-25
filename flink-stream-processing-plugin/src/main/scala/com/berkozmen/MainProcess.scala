package com.berkozmen

import com.berkozmen.Model.Order
import com.berkozmen.utils.{OrderDeserializationSchema, TupleToJsonSerializationSchema}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util.Properties
import java.sql.Timestamp
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

object MainProcess {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "kafka:9092")
    kafkaProps.setProperty("group.id", "consumer-group-1")

    val ordersSource = craeteSource(kafkaProps, "orders")
    val ordersStream = env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "Orders Source")
    val aggregatedStream = calcStreamAgg(ordersStream)

    val kafkaSink = createSink(kafkaProps)
    aggregatedStream.sinkTo(kafkaSink)

    env.execute("OrderProcessing")

  }

  private def createSink(kafkaProps: Properties): KafkaSink[(Long, Long, String, Timestamp)] = {
    KafkaSink.builder[(Long, Long, String, Timestamp)]()
      .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
        .setTopic("output")
        .setValueSerializationSchema(new TupleToJsonSerializationSchema())
        .build()
      )
      .setKafkaProducerConfig(kafkaProps)
      .build()
  }

  private def craeteSource(kafkaProps: Properties, topicName: String): KafkaSource[Order] = {
    KafkaSource.builder[Order]()
      .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
      .setTopics(topicName)
      .setGroupId(kafkaProps.getProperty("group.id"))
      .setValueOnlyDeserializer(new OrderDeserializationSchema())
      .setStartingOffsets(OffsetsInitializer.earliest())
      .build()
  }

  def calcStreamAgg(orders: DataStream[Order]): DataStream[(Long, Long, String, Timestamp)] = {
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[Order](Duration.ofMinutes(5)) // lag duration
      .withTimestampAssigner(new SerializableTimestampAssigner[Order] {
        override def extractTimestamp(element: Order, recordTimestamp: Long): Long = {
          parseDate(element.order_date).getOrElse(throw new RuntimeException("Invalid date format"))
        }
      })

    orders
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .map(order => (order.seller_id, order.product_id, order.location, parseDate(order.order_date)))
      .keyBy(_._3)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .apply((key, window, input, out: Collector[(Long, Long, String, Timestamp)]) => {
        val sellerCount = input.map(_._1).toSet.size
        val productCount = input.size
        out.collect((sellerCount, productCount, key, new Timestamp(window.getEnd)))
      })
  }

  private def parseDate(dateStr: String): Option[Long] = {
    try {
      val zonedDateTime = ZonedDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME)
      Some(zonedDateTime.toInstant.toEpochMilli) // timeStamp holds as Long value
    } catch {
      case e: DateTimeParseException =>
        println(s"Date parsing error: ${e.getMessage}")
        None
    }
  }

}
