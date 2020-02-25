package com.sparkbyexamples.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFromSnowflake extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  var sfOptions = Map(
    "sfURL" -> "https://oea81082.us-east-1.snowflakecomputing.com/",
    "sfAccount" -> "oea81082",
    "sfUser" -> "nnkumar13",
    "sfPassword" -> "#######1",
    "sfDatabase" -> "snowflake_sample_data",
    "sfSchema" -> "tpch_sf1",
  "sfRole" -> "ACCOUNTADMIN"
  )

  val df: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake")
    .options(sfOptions)
    .option("query", "SELECT l_returnflag,l_linestatus,sum(l_quantity) as sum_qty FROM lineitem GROUP BY l_returnflag,l_linestatus")
    .load()

  df.show(false)
}
