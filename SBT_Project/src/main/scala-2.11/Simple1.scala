/**
  * Created by nandini on 8/12/16.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Simple1 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()

      .master("local")
      .appName("APP1")

      .getOrCreate()


    import spark.implicits._

    val df = spark.read.option("header","true").option("inferSchema","true").csv("baby_names.csv")

    df.printSchema();
    //Number of Male and female in each year
     val df1=df.groupBy("Year","Sex").agg(sum("Count") as "No_M&F").orderBy(desc("Year"),desc("No_M&F")).show()

    //Number of babies in each County
    val df2=df.groupBy(upper($"County") as "County1").agg(sum("Count")).orderBy(asc("County1")).show()

    //Number of Males and females in each County
    val df3=df.groupBy(lower($"County") as "County1",lower($"Sex") as "Sex1").agg(sum("Count")).orderBy(asc("County1"),asc("Sex1")).show()

    //taking name as identifier
    val df4=df.orderBy(asc("First Name")).groupBy("First Name").agg(collect_set("County") as "Collection").select("Collection").show()

    //Maximum number of males born in particular year in particular country
    val df5=df.groupBy("Year","County","Sex").agg(max("Count") as "Count1").orderBy(asc("Year"),asc("County")).show()

     val df6=df.groupBy("Year","Sex","County").agg(max("Count") as "max_in_M&F")
    val df7=df.join(df6,Seq("Year","Sex","County")).show()


    def strLength( inputString: String) : String = {
      val a = if (inputString == "ST LAWRENCE") "yes" else "No"
      a
    }

      //spark.udf.register("Upper1",strLength(_String))
    val upperUDF =  udf[String,String](strLength)
    df.withColumn("is it a metro city or not?", upperUDF('County)).show
  //  val df.group

   /* df7.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("newBaby.csv")*/




  }


}
