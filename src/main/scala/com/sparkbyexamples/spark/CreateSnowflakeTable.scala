package com.sparkbyexamples.spark

import java.sql.DriverManager

object CreateSnowflakeTable extends App{

  val properties = new java.util.Properties()
  properties.put("user", "nnkumar13")
  properties.put("password", "######1")
  properties.put("account", "oea81082")
  properties.put("warehouse", "mywh")
  properties.put("db", "EMP")
  properties.put("schema", "public")
  properties.put("role","ACCOUNTADMIN")

  //JDBC connection string
  val jdbcUrl = "jdbc:snowflake://oea81082.us-east-1.snowflakecomputing.com/"

  println("Created JDBC connection")
  val connection = DriverManager.getConnection(jdbcUrl, properties)
  println("Done creating JDBC connection")

  println("Created JDBC statement")
  val statement = connection.createStatement

  // create a table
  println("Creating table EMPLOYEE")
  statement.executeUpdate("create or replace table EMPLOYEE(name VARCHAR, department VARCHAR, salary number)")
  statement.close
  println("Done creating EMPLOYEE table")

  connection.close()
  println("End connection")
}
