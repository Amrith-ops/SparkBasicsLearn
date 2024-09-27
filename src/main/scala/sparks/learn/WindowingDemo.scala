package sparks.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, sum}

object WindowingDemo extends Serializable {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("AggDemo").
      getOrCreate()

    val readData =  sparkSession.read.format("parquet")
      .option("inferSchema","true")
      .option("path","dataSink/assign/")
      .load()

    val windowStrategy = Window.partitionBy("Country")
      .orderBy("WeekNumber").
      rowsBetween(-2, Window.currentRow)

    readData.withColumn("RunningTotal",
      sum("TotalPrice").
        over(windowStrategy)
    ).show()

    val example2 = List(
      ("Lisa", "Sales", 10000, 35),
      ("Evan", "Sales", 32000, 38),
      ("Fred", "Engineering", 21000, 28),
      ("Alex", "Sales", 30000, 33),
      ("Tom", "Engineering", 23000, 33),
      ("Jane", "Marketing", 29000, 28),
      ("Jeff", "Marketing", 35000, 38),
      ("Paul", "Engineering", 29000, 23),
      ("Chloe", "Engineering", 23000, 25)

    )

    val salaryDf = sparkSession.createDataFrame(example2).toDF("Name","Dept","Salary","Age")

    /*
    1. PartionBy Dept column
    2. sortby salary
    3. define from which rows to start and end
     */

    val windowSalary = Window.partitionBy("Dept")
      .orderBy("Salary")

    salaryDf.withColumn("Salary_Rank",
      rank().over(windowSalary)
    ).show()


  }
}
