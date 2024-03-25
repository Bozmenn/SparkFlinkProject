package utils

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

class CollectSink extends SinkFunction[(Long, Long, String, Timestamp)] {
  override def invoke(value: (Long, Long, String, Timestamp)): Unit = {
    CollectSink.values += value
  }
}

object CollectSink {
  val values: ListBuffer[(Long, Long, String, Timestamp)] = ListBuffer.empty[(Long, Long, String, Timestamp)]
}
