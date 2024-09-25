package sparks.learn

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {
  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)
  println(getClass.getName)
  logger.info("Starting Hello spark")

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((key,value) => sparkConf.set(key.toString, value.toString))
    sparkConf
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate();
    if (args.length == 0) {
      logger.info("Send the arguments to main method")
      System.exit(1);
    }
    val dataFrame = readDataFrame(args(0),spark)
    val repartionData = dataFrame.repartition(2)
    val transformedData = transformDf(repartionData)
    // repartionData.show()
    // transformedData.show()
    logger.info(transformedData.collect().mkString("->"))
    logger.info("Ending Hello spark")
    Thread.sleep(2000000)
    spark.stop()
  }
  def transformDf(sourceData:DataFrame):DataFrame = {
    sourceData.where("Age < 40")
      .select("Country", "Age", "no_employees", "remote_work")
      .groupBy("Country").count()
  }
  def readDataFrame(args:String, spark:SparkSession): DataFrame = {
    val dataframe = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(args)
    dataframe
  }

}
