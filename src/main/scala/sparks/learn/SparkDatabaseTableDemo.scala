package sparks.learn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties
import scala.io.Source

object SparkDatabaseTableDemo extends Serializable {

  def getSparkConf: SparkConf = {
    val sparkConf = new SparkConf
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((key, value) => sparkConf.set(key.toString, value.toString))
    sparkConf
  }
    def main(args: Array[String]): Unit = {
      val sparkContext = SparkSession
        .builder()
        .config(getSparkConf).master("local[3]")
        .enableHiveSupport()
        .getOrCreate()

      val sparkReader = sparkContext.read.format("parquet")
        .option("path","data/flight-time.parquet").load()
      sparkContext.sql("CREATE DATABASE IF NOT EXISTS AMRITH_DB")
      sparkContext.catalog.setCurrentDatabase("AMRITH_DB")
      // since we know catalog store all info about spark tables i'm gonna use it to set the AMRITH_DB as current database
      sparkContext.catalog.listTables().show()
      sparkReader.write.format("csv").mode(SaveMode.Overwrite)
        .saveAsTable("flight_data")
      /*
       Before specifying the spark to write the data into given table we need to do two things
          1. create the database in which you want to store the table
          2. just like changing directory to current directory, set the Database to current database i.e
          the database you have just created

       */

      /*
      Result:
        we can see it creates two tables in the folder structure
          1. spark-warehouse which stores table data i.e flight_data table
          2. spark metadata catalog which stores metadata information
          3. we can do a partitionBy the table as well using columnName just like we have done for
          parquet file while writing to a file
       */
    }
}
