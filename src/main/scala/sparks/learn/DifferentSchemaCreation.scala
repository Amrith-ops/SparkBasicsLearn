package sparks.learn

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id, to_date}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.sql.Date


case class AttachSchema(SurName : String , Date: Int, Month: Int, Year: Int)
object DifferentSchemaCreation extends Serializable {

  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
     val sparkSession = SparkSession.builder()
       .master("local[4]")
       .appName("different schema")
       .getOrCreate()

     val data = List(("Amrith", 30,3,96),("Swathi", 26,5,96), ("Amma",1,1,50),("Amrith", 30,3,96))
     import  sparkSession.implicits._
     val schema = StructType(
       List(
         StructField("SurName", StringType),
         StructField("Date", IntegerType),
         StructField("Month", IntegerType),
         StructField("Year", IntegerType)
       )
     )
     // i can't use StructType schema here, since createDataFrame doesn't accept List of tuples
     // instead it accept List of Row objects in overloaded method where i can define StructType schema
     val df = sparkSession.createDataFrame(data).toDF("SurName","Date","Month","Year")
     println(df.printSchema())

     // For each Dataframe row if you want to have unique id then use monotonically_increasing_id function

     val rdf = df.withColumn("Id", monotonically_increasing_id())

     val cdf = correctYearDf(rdf)

     cdf.show()

     val fdf = concatDf(cdf)

     fdf.printSchema()
     val list = List("Date","Month", "Year")
     val dc = dropColumns(fdf, list:_*)

     dropDuplicat(dc, List("SurName", "Dob")).show()


  }

  def correctYearDf( df: DataFrame): DataFrame = {
    val cdf = df.withColumn("Year", expr(
      """
        |CASE WHEN Year < 20 THEN cast(year as int) + 2000
        |WHEN YEAR < 100 THEN cast(year as int) + 1900
        |ELSE YEAR
        |END
        |""".stripMargin
    ))
    cdf
  }

  def concatDf(df: DataFrame): DataFrame = {
    val cdf = df.withColumn(
      "Dob", to_date(expr("concat(Date,'/',Month,'/',Year)"),"d/M/y")
    )
    cdf.show()
    cdf
  }

  def dropColumns(dataFrame: DataFrame,args: String*): DataFrame = {
    dataFrame.drop(args:_*)
  }

  def dropDuplicat(dataFrame: DataFrame, args: List[String]) : DataFrame = {
    val duplicate = dataFrame.dropDuplicates(args)
    duplicate
  }

}
