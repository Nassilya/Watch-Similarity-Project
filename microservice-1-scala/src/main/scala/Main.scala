import scala.util.{Success, Failure}

object Main extends App {
  val csvPath = "../data/watches/metadata.csv"

  Parsing.loadFromCsv(csvPath) match {
    case Success(watches) =>
      Parsing.displaySummary(watches)

      println("\nProcessing first image...")
      ImageProcessing.process(watches.head.imagePath) match {
        case Some(_) => println(s"Image processed: ${watches.head.imagePath}")
        case None    => println(s"Image not found: ${watches.head.imagePath}")
      }

    case Failure(e) => println(s"Error: ${e.getMessage}")
  }
}