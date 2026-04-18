import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File

object Parsing {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("Parsing: loading CSV with Spark")

    val raw = spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("../data/watches/metadata.csv")

    val cols = raw.columns

    val df = raw
      .withColumnRenamed(cols(0), "id")
      .withColumnRenamed(cols(1), "brand")
      .withColumnRenamed(cols(2), "name")
      .withColumnRenamed(cols(3), "price")
      .withColumn("imagePath",      concat(lit("../data/watches/images/"),       col("id"), lit(".jpg")))
      .withColumn("processed_path", concat(lit("../preprocessed/images/"), col("id"), lit(".jpg")))
      .na.drop(Seq("id"))

    new File("../preprocessed").mkdirs()

    df.write
      .mode("overwrite")
      .parquet("../preprocessed/watches.parquet")

    val total = df.count()
    println(s"Parsing done: $total watches saved to ../preprocessed/watches.parquet")

    println("\nFirst 5 entries:")
    df.select("id", "brand", "name", "price").show(5, truncate = false)
  }
}
