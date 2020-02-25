package com.sparkbyexamples.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteEmpDataFrameToSnowflake extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate();

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Maria","Finance",3000),
    ("Raman","Finance",3000),
    ("Scott","Finance",3300),
    ("Jen","Finance",3900),
    ("Jeff","Marketing",3000),
    ("Kumar","Marketing",2000)
  )
  val df = simpleData.toDF("name","department","salary")
  df.show()

  var sfOptions = Map(
    "sfURL" -> "https://oea81082.us-east-1.snowflakecomputing.com/",
    "sfAccount" -> "oea81082",
    "sfUser" -> "nnkumar13",
    "sfPassword" -> "Nelamalli#1",
    "sfDatabase" -> "EMP",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "ACCOUNTADMIN"
  )

  df.write
    .format("snowflake")
    .options(sfOptions)
    .option("dbtable", "EMPLOYEE")
    .mode(SaveMode.Overwrite)
    .save()

}
