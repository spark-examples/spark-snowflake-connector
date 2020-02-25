package com.sparkbyexamples.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadEmpFromSnowflake extends App{

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  var sfOptions = Map(
    "sfURL" -> "https://oea81082.us-east-1.snowflakecomputing.com/",
    "sfAccount" -> "oea81082",
    "sfUser" -> "nnkumar13",
    "sfPassword" -> "Nelamalli#1",
    "sfDatabase" -> "EMP",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )

  val df: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake")
    .options(sfOptions)
    .option("dbtable", "EMPLOYEE")
    .load()

  df.show(false)

  val df1: DataFrame = spark.read
    .format("net.snowflake.spark.snowflake")
    .options(sfOptions)
    .option("query", "select department, sum(salary) as total_salary from EMPLOYEE group by department")
    .load()

  df1.show(false)
}
