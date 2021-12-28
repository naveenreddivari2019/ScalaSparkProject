import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object SparkSessionTest extends App{

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.master", "local")
    .getOrCreate()

  println("Spark Session : "+spark)

  var schema = new StructType()
    .add("admission_id", IntegerType, true)
    .add("patient_id", IntegerType, true)
    .add("admission_date", DateType, true)
    .add("discharge_date", DateType, true)

  val map=Map("header"->"true","inferSchema"-> "true")
  val df=spark.read.options(map).csv("C:/Users/Naveen/Downloads/UseCase/UseCase/Questions/Spark/Sacramentorealestatetransactions.csv")

  df.printSchema()
  df.show(10)

  spark.close()


}
