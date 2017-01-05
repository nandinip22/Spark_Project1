
/**
  * Created by nandini on 29/12/16.
  */
object Ebay {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  def main(args: Array[String]) {


    val spark = SparkSession.builder()
      .master("local")
      .appName("APP1")
      .getOrCreate()
    import spark.implicits._

    val data_frame1 = spark.read.option("header", "true").option("inferSchema", "true").csv("ebay.csv")



    data_frame1.printSchema()
    data_frame1.show(10)

    // How many auctions were held?
    val data_frame2=data_frame1.select("auctionid").distinct().count()
    println("Number of auctions held")
    println(data_frame2)
    //How many bids were made per item?
    val data_frame3=data_frame1.groupBy("auctionid","item").count().show()

    //how many min, max,avg number of bids per item?
    val data_frame4=data_frame1.groupBy("auctionid","item").count().agg(max("count"),min("count"),max("count")).show()

    val data_frame5=data_frame1.where("price>100").show()
    val squared = (s: Double) => {
      s * s
    }
    spark.udf.register("square", squared)
    val upperUDF = udf(squared)
    data_frame1.withColumn("double the price",upperUDF($"price")).show()

    /*def makeDT(date: String, time: String, tz: String) = s"$date $time $tz"
    spark.udf.register("makeDt", makeDT(_:String,_:String,_:String))

    // Now we can use our function directly in SparkSQL.
    spark.sql("SELECT amount, makeDt(date, time, tz) from df").take(2)
    // but not outside
    df.select($"customer_id", makeDt($"date", $"time", $"tz"), $"amount").take(2) // fails*/


  }
}
