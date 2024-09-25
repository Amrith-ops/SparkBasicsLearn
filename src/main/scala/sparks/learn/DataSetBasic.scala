package sparks.learn

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties
import scala.io.Source

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object DataSetBasic extends Serializable {
  @transient
  lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate();
    val dataframe:Dataset[Row]= readDataFrame(spark, args(0))
    import spark.implicits._
    val dataset: Dataset[SurveyRecord] = dataframe.select("Age", "Gender", "Country", "state")
      .as[SurveyRecord]
    val filteredDs = dataset.filter(survey => survey.Age < 40) // this is type safe catches error at compile time
    filteredDs.show()
    dataset.filter("ages < 40")
    // this is throwing error during runtime since ages column is not present i.e why type checking during compilation is important
    // this is not type safe even though it's dataset
    // because you need to use overloaded filter function which accepts lambda function as argument
    /*Hence to make type safety you need 1) explicit case class to type cast from dataframe to dataset
    2) you need to use function which accepts lambda function as argument
    */
  }

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((key, value) => sparkConf.set(key.toString, value.toString))
    sparkConf
  }

  def readDataFrame(sparkSession: SparkSession, path:String): DataFrame = {
    val sparkDf = sparkSession.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
    sparkDf
  }

}