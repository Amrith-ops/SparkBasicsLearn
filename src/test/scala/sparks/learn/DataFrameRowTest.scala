package sparks.learn

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import sparks.learn.DataFrameRow.changeDfSchema

import java.sql.Date

case class RowRecord(ID:String, EventDate:Date)
class DataFrameRowTest extends FunSuite with BeforeAndAfterAll {

  var spark:SparkSession = _
  @transient
  var df: DataFrame = _
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Hello Spark")
      .master("local[3]")
      .getOrCreate()
    val listRow = List(Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("125", "4/05/2020"))

    val rdd = spark.sparkContext.parallelize(listRow)

    val schema = StructType(
      List(
        StructField("ID", StringType),
        StructField("EventDate", StringType)
      )
    )

    df = spark.createDataFrame(rdd, schema)
  }
  test("check the data type of the spark column modified") {
      val cdf = changeDfSchema(df, "EventDate", "M/d/y").collectAsList()
      cdf.forEach(row => {
        assert(row.get(1).isInstanceOf[Date])
      })
  }
  val spark2 = spark
  import spark2.implicits._

  test("check the data value present in the spark column date") {
    val cdf = changeDfSchema(df, "EventDate", "M/d/y").as[RowRecord].collectAsList()
    cdf.forEach(
      row => {
        row.EventDate.toString == "4/5/2020"
      }
    )

  }

}
