import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession.builder()
    .appName("WatchSimilarity-MS1")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  Parsing.run(spark)
  ImageProcessing.run(spark)

  spark.stop()
  println("MS1 complete.")
}
