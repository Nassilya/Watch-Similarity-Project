import scala.io.Source
import scala.util.{Try, Success, Failure}
import java.io.File

case class Watch(id: String, brand: String, name: String, price: String, imagePath: String)

object Parsing {

  private val imagesDir = "../data/watches/images/"

  def loadFromCsv(filePath: String): Try[List[Watch]] = Try {
    val source = Source.fromFile(filePath)
    try {
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
  }

  def displaySummary(watches: List[Watch]): Unit = {
    println(s"Total watches loaded: ${watches.length}")
    println("\nFirst 5 entries:")
    watches.take(5).foreach { w =>
        println(s"  [${w.id}] ${w.brand} - ${w.name} - ${w.price}")
    }
  }
}