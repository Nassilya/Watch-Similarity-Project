import scala.util.{Success, Failure}

object Main extends App {
  val csvPath = "../data/watches/metadata.csv"

  Parsing.loadFromCsv(csvPath) match {
    case Success(watches) =>
      Parsing.displaySummary(watches)
      if (watches.nonEmpty) {
        new java.io.File("../preprocessed/metadata_preprocessed.csv").delete()
        watches.foreach { watch => ImageProcessing.process(watch) }
        println("All images processed and saved to ../preprocessed/")
      } else {
        println("No watches found in the CSV.")
      }
    case Failure(e) => println(s"Error: ${e.getMessage}")
  }
}
