import com.berkozmen.MainProcess
import com.berkozmen.Model.Order
import org.apache.flink.streaming.api.scala._
import org.junit.Test
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer

class MainProcessTest {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  @Test
  def testCalcStreamAgg(): Unit = {
    CollectSink.values.clear()
    val mockDataStream = createMockData()
    val resultStream = MainProcess.calcStreamAgg(mockDataStream)
    //resultStream.addSink(new CollectSink[(Long, Long, String, java.sql.Timestamp)])
    env.execute("Test CalcStreamAgg")

    println("Test Results:")
    CollectSink.values.foreach(println)
  }

  class CollectSink[T] extends SinkFunction[T] {
    override def invoke(value: T, context: SinkFunction.Context): Unit = {
      CollectSink.values.synchronized {
        CollectSink.values += value
      }
    }
  }

  object CollectSink {
    val values: ListBuffer[Any] = ListBuffer.empty[Any]
  }

  private def createMockData(): DataStream[Order] = {
    env.fromElements(
      Order(
        customer_id = "cust1",
        location = "location1",
        seller_id = "seller1",
        order_date = "2023-03-01",
        order_id = "order1",
        price = 99.99,
        product_id = "prod1",
        status = "shipped"
      ),
      Order(
        customer_id = "cust2",
        location = "location2",
        seller_id = "seller2",
        order_date = "2023-03-02",
        order_id = "order2",
        price = 199.99,
        product_id = "prod2",
        status = "delivered"
      ),
      Order(
        customer_id = "cust3",
        location = "location3",
        seller_id = "seller3",
        order_date = "2023-03-03",
        order_id = "order3",
        price = 299.99,
        product_id = "prod3",
        status = "processing"
      ),
    )
  }
}
