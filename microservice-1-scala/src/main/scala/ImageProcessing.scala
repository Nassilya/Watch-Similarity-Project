import java.io.{File, PrintWriter, FileWriter}
import java.awt.image.BufferedImage
import java.awt.RenderingHints
import javax.imageio.ImageIO
import scala.util.Try

object ImageProcessing {

  // Configuration des chemins (Hardcoded comme demandé)
  val TARGET_SIZE = 224
  private val InputImagesDir  = "../data/watches/images/"
  private val OutputImagesDir = "../data/preprocessed/images/"
  private val OutputCsvPath   = "../data/preprocessed/metadata_preprocessed.csv"

  /**
   * Redimensionne une image en 224x224 avec un filtre bilinéaire
   */
  def resize(image: BufferedImage): BufferedImage = {
    val resized = new BufferedImage(TARGET_SIZE, TARGET_SIZE, BufferedImage.TYPE_INT_RGB)
    val g = resized.createGraphics()
    g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
    g.drawImage(image, 0, 0, TARGET_SIZE, TARGET_SIZE, null)
    g.dispose()
    resized
  }

  /**
   * Crée les répertoires nécessaires s'ils n'existent pas
   */
  private def prepareDirectories(): Unit = {
    val dir = new File(OutputImagesDir)
    if (!dir.exists()) dir.mkdirs()
  }

  /**
   * Sauvegarde une ligne dans le CSV de liaison pour Python
   */
  private def saveToMetadata(watchId: String, brand: String, name: String, finalPath: String): Unit = {
    val fileExists = new File(OutputCsvPath).exists()
    val fw = new FileWriter(OutputCsvPath, true)
    val writer = new PrintWriter(fw)
    try {
      if (!fileExists) writer.println("id,brand,name,processed_path")
      writer.println(s"$watchId,$brand,$name,$finalPath")
    } finally {
      writer.close()
    }
  }

  /**
   * Processus complet : Lecture -> Resize -> Sauvegarde Image -> Log CSV
   */
  def process(watch: Watch): Unit = {
    prepareDirectories()
    
    val inputFile = new File(watch.imagePath)
    val outputFileName = s"${watch.id}.jpg"
    val outputPath = s"$OutputImagesDir$outputFileName"

    if (inputFile.exists()) {
      try {
        val image = ImageIO.read(inputFile)
        if (image != null) {
          // 1. Redimensionner
          val resized = resize(image)
          
          // 2. Sauvegarder l'image sur le disque
          ImageIO.write(resized, "jpg", new File(outputPath))
          
          // 3. Ajouter au CSV de liaison
          saveToMetadata(watch.id, watch.brand, watch.name, outputPath)
          
          println(s"✅ [${watch.id}] Traitée avec succès")
        }
      } catch {
        case e: Exception => println(s"❌ Erreur sur l'image ${watch.id}: ${e.getMessage}")
      }
    } else {
      println(s"⚠️ Image source introuvable : ${watch.imagePath}")
    }
  }
}