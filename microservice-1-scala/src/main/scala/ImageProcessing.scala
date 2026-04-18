import org.apache.spark.sql.SparkSession
import java.io.File
import java.awt.RenderingHints
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

object ImageProcessing {

  def run(spark: SparkSession): Unit = {
    println("ImageProcessing: resizing images with Spark RDD")

    val df = spark.read.parquet("../preprocessed/watches.parquet")

    val records = df.select("id", "imagePath", "processed_path").collect()

    new File("../preprocessed/images").mkdirs()

    val sc = spark.sparkContext
    val recordsRDD = sc.parallelize(records.toSeq)

    recordsRDD.foreach { row =>
      val id            = row.getString(0)
      val imagePath     = row.getString(1)
      val processedPath = row.getString(2)

      val inputFile = new File(imagePath)
      if (inputFile.exists()) {
        try {
          val src = ImageIO.read(inputFile)
          if (src != null) {
            val resized = new BufferedImage(224, 224, BufferedImage.TYPE_INT_RGB)
            val g = resized.createGraphics()
            g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
            g.drawImage(src, 0, 0, 224, 224, null)
            g.dispose()
            ImageIO.write(resized, "jpg", new File(processedPath))
            println(s"[$id] resized")
          }
        } catch {
          case e: Exception => println(s"Error on $id: ${e.getMessage}")
        }
      } else {
        println(s"Image not found: $imagePath")
      }
    }

    println(s"ImageProcessing done: ${records.length} images processed.")
  }
}
