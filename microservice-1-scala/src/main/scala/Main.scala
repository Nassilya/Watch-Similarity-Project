import scala.util.{Success, Failure}

object Main extends App {
  val csvPath = "../data/watches/metadata.csv"

  Parsing.loadFromCsv(csvPath) match {
    case Success(watches) =>
      Parsing.displaySummary(watches)

      if (watches.nonEmpty) {
        println("\nProcessing all images...")
        
        // On boucle sur toutes les montres
        watches.foreach { watch =>
          // On passe l'objet 'watch' entier, pas juste le chemin !
          ImageProcessing.process(watch)
        }
        
        println("\nAll images processed and saved to ../data/preprocessed/")
      } else {
        println("No watches found in the CSV.")
      }

    case Failure(e) => println(s"Error: ${e.getMessage}")
  }
}