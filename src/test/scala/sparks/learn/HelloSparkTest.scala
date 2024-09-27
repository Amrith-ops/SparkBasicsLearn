package sparks.learn

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sparks.learn.HelloSpark.{readDataFrame,transformDf}

class HelloSparkTest extends FunSuite with BeforeAndAfterAll{

  @transient
  var spark:SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Hello Spark")
      . master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("test the total number of rows in dataframe") {
    val testData = readDataFrame("data/sample.csv", spark)
    val count = testData.count()
    assert(count==9)
  }

  test("count groupBy country") {
    val testData = readDataFrame("data/sample.csv", spark)
    val groupedCount= transformDf(testData)

  }

}
