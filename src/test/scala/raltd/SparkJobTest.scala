package raltd

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

/**
  * Created by Rahul on 11/09/2019.
  */
class SparkJobTest extends FunSuite with DataFrameSuiteBase with SharedSparkContext {

  test("testProcess") {
    testProcess
  }

  def testProcess(): Unit ={
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db LOCATION '/tmp/test_db.db'")
    spark.sql("DROP TABLE IF EXISTS test_db.customer")
    //spark.sql("CREATE TABLE  test_db.customer(name string, age int)")

    val customerSchema = StructType(Array(StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)))

    val df = spark.read.schema(customerSchema).option("header", "true").option("inferSchema", "false").csv("src/test/resources/customer.csv")
    df.write.saveAsTable("customer")

//    val df2 = spark.sql("select * from customer")
//    println("------------------------------------------------------------------")
//    println(df2.count())
    println("-----------------------------REAL TEST-------------------------------------")
    println(new SparkJob(spark).process())
    println("------------------------------------------------------------------")
  }
}
