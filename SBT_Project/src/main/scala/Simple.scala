
/**
  * Created by nandini on 8/12/16.
  */

object Simple {

  def main(args: Array[String]) {
    import org.apache.spark.{SparkConf, SparkContext}
    val logFile = "home/nandini/Desktop/Untitled Document 1.txt"// Should be some file on your system
    val conf = new SparkConf().setAppName("Simple")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}



