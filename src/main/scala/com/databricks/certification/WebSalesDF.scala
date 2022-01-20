package com.databricks.certification
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import com.databricks.certification.SparkSessionObject.SparkSessionObject
object WebSalesDF {
  def main(args:Array[String]): Unit ={
    println("************WebSalesDF****************")
    val spark=new SparkSessionObject("CustomerDF Spark Job").SessionObject()
    spark.conf.set("spark.sql.shuffle.partitions",10)
    val csvFileReadOptions = Map("header" -> "true"
      ,"delimiter" -> ","
      ,"inferSchema" -> "false")
    val datFileReadOptions = Map("header" -> "true"
      ,"delimiter" -> "|"
      ,"inferSchema" -> "false")

    val webSalesSchema = "ws_sold_date_sk LONG,ws_sold_time_sk LONG,ws_ship_date_sk LONG,ws_item_sk LONG,ws_bill_customer_sk LONG,ws_bill_cdemo_sk LONG,ws_bill_hdemo_sk LONG,ws_bill_addr_sk LONG,ws_ship_customer_sk LONG,ws_ship_cdemo_sk LONG,ws_ship_hdemo_sk LONG,ws_ship_addr_sk LONG,ws_web_page_sk LONG,ws_web_site_sk LONG,ws_ship_mode_sk LONG,ws_warehouse_sk LONG,ws_promo_sk LONG,ws_order_number LONG,ws_quantity INT,ws_wholesale_cost decimal(7,2),ws_list_price decimal(7,2),ws_sales_price decimal(7,2),ws_ext_discount_amt decimal(7,2),ws_ext_sales_price decimal(7,2),ws_ext_wholesale_cost decimal(7,2),ws_ext_list_price decimal(7,2),ws_ext_tax decimal(7,2),ws_coupon_amt decimal(7,2),ws_ext_ship_cost decimal(7,2),ws_net_paid decimal(7,2),ws_net_paid_inc_tax decimal(7,2),ws_net_paid_inc_ship decimal(7,2),ws_net_paid_inc_ship_tax decimal(7,2),ws_net_profit decimal(7,2)"
    val customerDfSchema_DDL = "address_id INT,birth_country STRING,birthdate date,customer_id INT,demographics STRUCT<buy_potential: STRING, credit_rating: STRING, education_status: STRING, income_range: ARRAY<INT>, purchase_estimate: INT, vehicle_count: INT>,email_address STRING,firstname STRING,gender STRING,is_preffered_customer STRING,lastname STRING,salutation STRING"
    val itemSchema = "i_item_sk LONG,i_item_id STRING,i_rec_start_date date,i_rec_end_date date,i_item_desc STRING,i_current_price decimal(7,2),i_wholesale_cost decimal(7,2),i_brand_id INT,i_brand STRING,i_class_id INT,i_class STRING,i_category_id INT,i_category STRING,i_manufact_id INT,i_manufact STRING,i_size STRING,i_formulation STRING,i_color STRING,i_units STRING,i_container STRING,i_manager_id INT,i_product_name STRING"
    //Read Web Sales data
    val webSalesDf = spark.read.
      options(csvFileReadOptions)
      .schema(webSalesSchema)
      .csv("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\web_sales.csv")

    //Read address data
    val addressDf = spark.read.parquet("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\address.parquet")

    //Read Customer data
    val customerDf = spark.read.format("json")
      .schema(customerDfSchema_DDL)
      .load("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\customer.json")

    //Read items data
    val itemDf = spark.read.
      options(datFileReadOptions)
      .schema(itemSchema)
      .csv("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\item.dat")
  //persist data frame
    customerDf.persist(StorageLevel.MEMORY_AND_DISK_2)

    webSalesDf.show(10)
    val customerPurchases = webSalesDf.selectExpr("ws_bill_customer_sk customer_id","ws_item_sk item_id")
    customerPurchases.show(5)

    val customerPurchase_GBY=customerPurchases.groupBy(col("customer_id")).agg(count("item_id").alias("item_count"))
    customerPurchase_GBY.show(5)

    webSalesDf.agg(
      max("ws_sales_price"),
      min("ws_sales_price"),
      avg("ws_sales_price"),
      mean("ws_sales_price"),
      count("ws_sales_price")
    ).show

    val customerWithAddress =
      customerDf.join(addressDf, customerDf.col("address_id") === addressDf.col("address_id"),"inner")
        .select("customer_id"
          ,"firstname"
          ,"lastname"
          ,"demographics.education_status"
          ,"location_type"
          ,"country"
          ,"city"
          ,"street_name"
        )

    customerWithAddress.write.mode(SaveMode.Overwrite).parquet("C:\\Users\\Naveen\\IdeaProjects\\ScalaSparkProject\\resources\\data\\write\\")


    println("Partitions ---------------->"+customerDf.rdd.getNumPartitions)
  }
}
