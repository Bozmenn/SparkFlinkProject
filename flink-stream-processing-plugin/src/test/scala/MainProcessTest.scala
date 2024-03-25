import com.berkozmen.MainProcess
import com.berkozmen.Model.Order
import org.apache.flink.streaming.api.scala._
import org.junit.Test
import utils.CollectSink

class MainProcessTest {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val collectSink = new CollectSink()

  @Test
  def testCalcStreamAgg(): Unit = {
    val mockDataStream = createMockData()
    val resultStream = MainProcess.calcStreamAgg(mockDataStream)
    resultStream.addSink(collectSink)

    env.execute("Test CalcStreamAgg")

    println("Test Results:")
    CollectSink.values.foreach(println)

    CollectSink.values.clear()
  }

  private def createMockData(): DataStream[Order] = {
    env.fromElements(
      Order("cust1", "location1", "seller1", "2021-01-01T00:01:39.000+03:00", "order1", 99.99, "prod1", "shipped"),
      Order("cust2", "location2", "seller2", "2021-01-01T00:02:39.000+03:00", "order2", 199.99, "prod2", "delivered"),
      Order("cust3", "location3", "seller1", "2021-01-01T00:03:39.000+03:00", "order3", 299.99, "prod3", "processing"),
      Order("cust4", "location1", "seller2", "2021-01-01T00:04:39.000+03:00", "order4", 399.99, "prod1", "shipped"),
      Order("cust5", "location2", "seller3", "2021-01-01T00:05:39.000+03:00", "order5", 499.99, "prod2", "delivered"),
      Order("cust6", "location3", "seller1", "2021-01-01T00:06:39.000+03:00", "order6", 599.99, "prod3", "processing"),
      Order("cust7", "location1", "seller2", "2021-01-01T00:07:39.000+03:00", "order7", 699.99, "prod1", "shipped"),
      Order("cust8", "location2", "seller3", "2021-01-01T00:08:39.000+03:00", "order8", 799.99, "prod2", "delivered"),
      Order("cust9", "location3", "seller1", "2021-01-01T00:09:39.000+03:00", "order9", 899.99, "prod3", "processing"),
      Order("cust10", "location1", "seller2", "2021-01-01T00:10:39.000+03:00", "order10", 999.99, "prod1", "shipped")
    )
  }
}
