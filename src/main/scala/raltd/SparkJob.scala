package raltd

import org.apache.spark.sql.SparkSession

/**
  * Created by Rahul on 11/09/2019.
  */
class SparkJob(spark: SparkSession) {


  def process():Long = {
    val df = spark.sql("select * from customer")
    df.count()
  }
}
