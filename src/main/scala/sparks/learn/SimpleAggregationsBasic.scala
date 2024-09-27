package sparks.learn

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, expr, to_date, weekofyear}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object SimpleAggregationsBasic extends Serializable {
  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("AggDemo").
      getOrCreate()
    val read = readData(sparkSession)

    simpleAgg(read)

    read.createOrReplaceTempView("invoices")

    sparkSession.sql(
      """
        |SELECT COUNTRY,InvoiceNo, sum(Quantity) as Total_Quantity, round(sum(Quantity*UnitPrice),2) as Total_Price
        |FROM Invoices
        |GROUP BY COUNTRY, InvoiceNo
        |""".stripMargin).show()

    excercise_assgn(read, sparkSession)

  }
  def excercise_assgn(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
    /*
    1. the date column is not cleaned a) it's string 2) another float value got added in the date column
    2. So i need to pick the date column individually and work on it for that i will pick the InvoiceDate
    and convert the dataframe into rdd.
    3. map each row and get the date part in the string
    4. after processing convert the rdd back to the dataframe
    5. Now i need to join the cleaned dataframe to the original data frame using the inner join and customerId
    6.Convert the date which is in String type to date type
    7. apply group by and get the result
     */
    import sparkSession.implicits._

    val fetchDate: RDD[Row] = dataFrame.select("InvoiceDate","CustomerID")
      .where("CustomerId IS NOT NULL").rdd
    val dateVal:RDD[Row] = fetchDate.map(
      row => {
          Row(row.getString(0).split(" ")(0), row.getInt(1))
      }
    )
    val schema = StructType(List(
      StructField("Extract",StringType),
      StructField("CustomerID",IntegerType)
    ))
    val dateDf = sparkSession.createDataFrame(dateVal, schema)
    println("after dropping "+dateDf.count())
    dateVal.take(5).foreach(println)

    dataFrame.createOrReplaceTempView("invoice_parent")
    dateDf.createOrReplaceTempView("extracted_date")
    val joinedDf = sparkSession.sql(
      """
        |SELECT * FROM invoice_parent
        | INNER JOIN extracted_date ON invoice_parent.CustomerID = extracted_date.CustomerID
        |""".stripMargin)
    joinedDf.printSchema()

    val invoiceDateDf: DataFrame = joinedDf.withColumn("InvoiceDate", to_date(col("Extract"),"dd-MM-yyyy"))
    val dateFiltered = invoiceDateDf.where("year(InvoiceDate)== 2010")
    val newDataFrame = dateFiltered.withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
    newDataFrame.createOrReplaceTempView("Invoices_Assign")

    val finalSolution = sparkSession.sql(
      """
        |SELECT COUNTRY, WeekNumber,
        |sum(Quantity) as TotalQuantity, round(sum(Quantity*UnitPrice),2) as TotalPrice
        |FROM Invoices_Assign GROUP BY Country, WeekNumber
        |""".stripMargin)

    // finalSolution.repartition(5)
    finalSolution.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path","dataSink/assign/")
      .save()

  }
  def readData(sparkSession: SparkSession) : DataFrame = {
    val readDf = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("path", "data/invoices.csv")
      .load()
    readDf
  }
  def simpleAgg(readDf: DataFrame) : DataFrame = {

    readDf.selectExpr(
      "count(1) as total_count",
      "count(StockCode) as total_Stocks",
      "sum(quantity) as Quantity_sum",
      "avg(UnitPrice) as avg_price"
    ).show()

    readDf

  }

}
