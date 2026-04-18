import org.apache.spark.sql.SparkSession
import java.io.{File, PrintWriter}

object Scoring {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WatchSimilarity-MS2")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("MS2: Scoring with Spark RDD")

    println("Loading embeddings...")
    val embDF = spark.read
      .option("header", "true")
      .csv("../embeddings/embeddings.csv")

    val embData = embDF.collect().map { row =>
      val id = row.getString(0)
      val features = (1 until row.length).map(i => row.getString(i).toDouble).toArray
      (id, features)
    }

    val ids = embData.map(_._1)
    val embeddings = embData.map(_._2)
    val n = ids.length

    println("Loading metadata...")
    val metaDF = spark.read.parquet("../preprocessed/watches.parquet")
    val metaMap = metaDF.collect().map { row =>
      val id = row.getAs[String]("id")
      val brand = row.getAs[String]("brand")
      val name = row.getAs[String]("name")
      val price = row.getAs[String]("price")
      id -> (brand, name, price)
    }.toMap

    println(s"Computing similarities for $n watches...")

    val sc = spark.sparkContext
    val embsBc = sc.broadcast(embeddings)
    val idsBc = sc.broadcast(ids)

    val indicesRDD = sc.parallelize(0 until n)

    val rows = indicesRDD.map { i =>
      val embs = embsBc.value
      val idsArr = idsBc.value
      val total = idsArr.length

      val scores = (0 until total)
        .filter(_ != i)
        .map { j =>
          val a = embs(i)
          val b = embs(j)
          var dot = 0.0
          var normA = 0.0
          var normB = 0.0
          var k = 0
          while (k < a.length) {
            dot += a(k) * b(k)
            normA += a(k) * a(k)
            normB += b(k) * b(k)
            k += 1
          }
          val sim = if (normA == 0 || normB == 0) 0.0 else dot / (math.sqrt(normA) * math.sqrt(normB))
          (j, sim)
        }
        .sortBy(-_._2)
        .take(5)

      val simIds = scores.map { case (j, _) => idsArr(j) }.mkString(";")
      val simScores = scores.map { case (_, s) =>
        f"${s * 100}%.1f"
      }.mkString(";")

      (idsArr(i), simIds, simScores)
    }.collect()

    new File("../output").mkdirs()
    val writer = new PrintWriter(new File("../output/final_results.csv"))
    try {
      writer.println("id,brand,name,price,imagePath,similar_ids,similar_scores")
      rows.foreach { case (id, simIds, simScores) =>
        val (brand, name, price) = metaMap.getOrElse(id, ("", "", ""))
        val imagePath = s"../data/watches/images/$id.jpg"
        writer.println(s""""$id","$brand","$name","$price","$imagePath","$simIds","$simScores"""")
      }
    } finally writer.close()

    spark.stop()
    println(s"Scoring complete! ${rows.length} watches saved to ../output/final_results.csv")
  }
}
