package sparks.learn

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import java.util.Properties
import scala.io.Source

object DataFrameReadSource extends Serializable {
  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  val schema = StructType(
    List(
      StructField("FL_DATE", DateType),
      StructField("OP_CARRIER", StringType),
      StructField("OP_CARRIER_FL_NUM", IntegerType),
      StructField("ORIGIN", StringType),
      StructField("ORIGIN_CITY_NAME", StringType),
      StructField("DEST", StringType),
      StructField("DEST_CITY_NAME", StringType),
      StructField("CRS_DEP_TIME", IntegerType),
      StructField("DEP_TIME", IntegerType),
      StructField("WHEELS_ON", IntegerType),
      StructField("TAXI_IN", IntegerType),
      StructField("CRS_ARR_TIME", IntegerType),
      StructField("ARR_TIME", IntegerType),
      StructField("CANCELLED", IntegerType),
      StructField("DISTANCE", IntegerType)

    )
  )
  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((key, value) => sparkConf.set(key.toString, value.toString))
    sparkConf
  }

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate();

    val csvReader = spark.read
      .format("csv")
      .option("header", "true")
      .option("path","data/flight-time.csv")
      //.option("inferSchema","true")
      .schema(schema)
      .load()
    logger.info("CSV printSchema "+csvReader.printSchema().toString)

    val jsonReader = spark.read
      .format("json")
      //.option("header", "true")
      .option("path", "data/flight-time.json")
      //.option("inferSchema", "true")
      .load()
    logger.info("json printSchema "+jsonReader.printSchema().toString)

    val parquetReader = spark.read
      .format("parquet")
      //.option("header", "true")
      .option("path", "data/flight-time.parquet")
      //.option("inferSchema", "true")
      .load()
    logger.info("parquet printSchema " + parquetReader.printSchema().toString)
  }

}
