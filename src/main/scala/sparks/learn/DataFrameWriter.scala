package sparks.learn

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties
import scala.io.Source

object DataFrameWriter extends Serializable {

  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)

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

    val parquetDf = readParquet(spark, "data/flight-time.parquet")
    logger.info("Before partitioning data: "+ parquetDf.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.spark_partition_id
    val partitionedCount = parquetDf.groupBy(spark_partition_id()).count()
    logger.info("How much data present in each partition before explicit partioning "+ partitionedCount.show())
    val repartitonedData = parquetDf.repartition(2)
    val repartitionedCount = repartitonedData.groupBy(spark_partition_id()).count()
    logger.info("How much data present in each partition after explicit partioning "+ repartitionedCount.show())
    writeParquet(repartitonedData)


  }

  def readParquet(sparkSession: SparkSession, path:String): DataFrame = {
    val parquetReader = sparkSession.read
      .format("parquet")
      //.option("header", "true")
      .option("path", "data/flight-time.parquet")
      //.option("inferSchema", "true")
      .load()
    logger.info("parquet printSchema " + parquetReader.printSchema().toString)
    parquetReader
  }

  def writeParquet(readDataFrame: DataFrame): Unit = {
    readDataFrame
      .write.format("avro")
      .mode(SaveMode.Overwrite)
      .option("path","dataSink/avro/")
      .save()

    readDataFrame.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .option("path","dataSink/json/")
      .partitionBy("OP_CARRIER","ORIGIN")
      .save()
  }
}
