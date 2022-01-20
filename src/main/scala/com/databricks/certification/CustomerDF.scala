package com.databricks.certification
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.databricks.certification.SparkSessionObject.SparkSessionObject
object CustomerDF {

  def main(args:Array[String]): Unit ={
    println("************CustomerDF****************")
    val spark=new SparkSessionObject("CustomerDF Spark Job").SessionObject()

    val customerDfSchema_DDL="address_id INT,birth_country STRING,birthdate date,customer_id INT,demographics STRUCT<buy_potential: STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,is_preffered_customer STRING,lastname STRING,salutation STRING"
    val customerDfSchema_ST=StructType(
      Array(
        StructField("address_id",IntegerType,true),
        StructField("birth_country",StringType,true),
        StructField("birthdate",DateType,true),
        StructField("customer_id",IntegerType,true),
        StructField("demographics",
          StructType(Array(
            StructField("buy_potential",StringType,true),
            StructField("credit_rating",StringType,true),
            StructField("education_status",StringType,true),
            StructField("income_range",ArrayType(IntegerType,true),true),
            StructField("purchase_estimate",IntegerType,true),
            StructField("vehicle_count",IntegerType,true))
          ),true),
        StructField("email_address",StringType,true),
        StructField("firstname",StringType,true),
        StructField("gender",StringType,true),
        StructField("is_preffered_customer",StringType,true),
        StructField("lastname",StringType,true),
        StructField("salutation",StringType,true)
      ))
    val customerDf=spark.read.schema(customerDfSchema_ST).json("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\customer.json")
    customerDf.printSchema()
    //customerDf.columns.length
    val customerDF_proj=customerDf.select(col("firstname"),column("lastname"),expr("year(birthdate) BirthYear"))
      customerDF_proj.show(5)

    //val customerDf_Casting=customerDf.select(col("birthdate").cast("String"))
    val customerDf_Casting=customerDf.selectExpr("cast(birthdate as String)")
    customerDf_Casting.printSchema()

    val customer_Df_RC=customerDf.drop()


    val customer_Df_Sort=customerDf.na.drop(how="all").sort(col("firstname").desc).filter(dayofmonth(col("birthdate"))>20)
    customer_Df_Sort.show(10)

    val filtered = customerDf
      .where(year(col("birthdate"))>1980)
      .filter(month(col("birthdate")) === 1 )
      .where("day(birthdate) > 15")
      .filter(col("birth_country" )=!= "UNITED STATES" )
      .select("firstname","lastname","birthdate","birth_country")
      filtered.explain()
    filtered.show(10)

  }
}
