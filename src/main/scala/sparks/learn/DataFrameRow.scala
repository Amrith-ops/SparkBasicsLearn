package sparks.learn

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameRow extends Serializable {

  def main(args:Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("data frame row")
      .getOrCreate()

    val listRow = List(Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("125", "4/05/2020"))

    val rdd = sparkSession.sparkContext.parallelize(listRow)

    val schema = StructType(
      List(
        StructField("ID", StringType),
        StructField("EventDate", StringType)
      )
    )

    val df = sparkSession.createDataFrame(rdd,schema)

    println("schema before change "+df.printSchema())

    val cdf = changeDfSchema(df,"EventDate", "M/d/y")

    println("schema after change "+cdf.printSchema())
  }

  def changeDfSchema(df:DataFrame, columnName:String, fmt:String): DataFrame = {
    val cdf = df.withColumn(columnName,to_date(col(columnName),fmt))
    cdf
  }
}
