import scala.io.Source
import scala.util.{Try, Success, Failure}
import java.io.{File, PrintWriter}

case class Watch(id: String, brand: String, name: String, price: String, imagePath: String)

object Parsing {

  private val imagesDir = "../data/watches/images/"
  private val outputCsvPath = "../data/parsing/processed_watches.csv"

  def loadFromCsv(filePath: String): Try[List[Watch]] = Try {
    val source = Source.fromFile(filePath)
    val watches = try {
      source.getLines().toList.tail.map { line =>
        val cols = line.split(",").map(_.trim)
        Watch(
          id        = cols(0),
          brand     = cols(1),
          name      = cols(2),
          price     = cols(3),
          imagePath = s"$imagesDir${cols(0)}.jpg"
        )
      }
    } finally source.close()

    // --- ÉTAPE DE STOCKAGE AUTOMATIQUE ---
    saveToCsv(watches, outputCsvPath) match {
      case Success(_) => println(s"Sauvegarde auto réussie dans : $outputCsvPath")
      case Failure(e) => println(s"Échec de la sauvegarde auto : ${e.getMessage}")
    }
    // -------------------------------------

    watches
  }

  // Méthode interne pour gérer l'écriture
  private def saveToCsv(watches: List[Watch], path: String): Try[Unit] = Try {
    val writer = new PrintWriter(new File(path))
    try {
      writer.println("id,brand,name,price,imagePath") // Header
      watches.foreach { w =>
        writer.println(s"${w.id},${w.brand},${w.name},${w.price},${w.imagePath}")
      }
    } finally {
      writer.close()
    }
  }

  def displaySummary(watches: List[Watch]): Unit = {
    println(s"Total watches loaded: ${watches.length}")
    println("\nFirst 5 entries:")
    watches.take(5).foreach { w =>
        println(s"  [${w.id}] ${w.brand} - ${w.name} - ${w.price}")
    }
  }
}