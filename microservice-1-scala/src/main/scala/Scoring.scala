import scala.io.Source
import java.io.{File, PrintWriter}
import scala.collection.parallel.CollectionConverters._

object Scoring {

  def main(args: Array[String]): Unit = run()

  def cosineSimilarity(a: Array[Double], b: Array[Double]): Double = {
    var dot = 0.0; var normA = 0.0; var normB = 0.0; var i = 0
    while (i < a.length) {
      dot += a(i) * b(i)
      normA += a(i) * a(i)
      normB += b(i) * b(i)
      i += 1
    }
    if (normA == 0 || normB == 0) 0.0
    else dot / (math.sqrt(normA) * math.sqrt(normB))
  }

  def parseCSVLine(line: String): Array[String] = {
    val result = scala.collection.mutable.ArrayBuffer[String]()
    var inQuotes = false
    val current = new StringBuilder
    for (c <- line) {
      if (c == '"') inQuotes = !inQuotes
      else if (c == ',' && !inQuotes) { result += current.toString; current.clear() }
      else current += c
    }
    result += current.toString
    result.toArray
  }

  def run(): Unit = {
    println("Loading embeddings...")
    val embSource = Source.fromFile("../embeddings/embeddings.csv")
    val embLines = embSource.getLines().toList
    embSource.close()

    val data = embLines.tail.map { line =>
      val cols = line.split(",")
      (cols(0), cols.tail.map(_.toDouble))
    }

    val ids = data.map(_._1).toArray
    val embeddings = data.map(_._2).toArray
    val n = ids.length

    println("Loading metadata...")
    val metaSource = Source.fromFile("../data/watches/metadata.csv")
    val metaLines = metaSource.getLines().toList
    metaSource.close()

    val metaMap = metaLines.tail.map { line =>
      val cols = parseCSVLine(line)
      val id    = cols(0)
      val brand = if (cols.length > 1) cols(1) else ""
      val name  = if (cols.length > 2) cols(2) else ""
      val price = if (cols.length > 3) cols(3) else ""
      id -> (brand, name, price)
    }.toMap

    println(s"Computing similarity for $n watches...")

    val rows = (0 until n).par.map { i =>
      val scores = (0 until n)
        .filter(_ != i)
        .map(j => (j, cosineSimilarity(embeddings(i), embeddings(j))))
        .sortBy(-_._2)
        .take(5)

      val simIds    = scores.map { case (j, _) => ids(j) }.mkString(";")
      val simScores = scores.map { case (_, s) => String.format(java.util.Locale.US, "%.1f", s * 100) }.mkString(";")

      val (brand, name, price) = metaMap.getOrElse(ids(i), ("", "", ""))
      val imagePath = s"../data/watches/images/${ids(i)}.jpg"

      s""""${ids(i)}","$brand","$name","$price","$imagePath","$simIds","$simScores""""
    }.toIndexedSeq

    new File("../output").mkdirs()
    val writer = new PrintWriter(new File("../output/final_results.csv"))

    try {
      writer.println("id,brand,name,price,imagePath,similar_ids,similar_scores")
      rows.foreach(writer.println)
    } finally {
      writer.close()
    }

    println("Scoring complete! Output saved to ../output/final_results.csv")
  }
}
