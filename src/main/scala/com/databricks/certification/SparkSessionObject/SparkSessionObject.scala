package com.databricks.certification.SparkSessionObject
import org.apache.spark.sql.SparkSession
class SparkSessionObject(name:String) {
  def SessionObject(): SparkSession ={
    var sparkObj=SparkSession
      .builder()
      .appName(name)
      .config("spark.master", "local")
      .getOrCreate()
    return sparkObj
  }
}
