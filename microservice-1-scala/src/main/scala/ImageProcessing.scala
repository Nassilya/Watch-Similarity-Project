import java.io.File
import java.awt.image.BufferedImage
import java.awt.RenderingHints
import javax.imageio.ImageIO

object ImageProcessing {

  val TARGET_SIZE = 224  

  def resize(image: BufferedImage): BufferedImage = {
    val resized = new BufferedImage(TARGET_SIZE, TARGET_SIZE, BufferedImage.TYPE_INT_RGB)
    val g = resized.createGraphics()
    g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
    g.drawImage(image, 0, 0, TARGET_SIZE, TARGET_SIZE, null)
    g.dispose()
    resized
  }

  def normalize(image: BufferedImage): Array[Array[Array[Float]]] = {
    val pixels = Array.ofDim[Float](3, TARGET_SIZE, TARGET_SIZE)
    for {
      y <- 0 until TARGET_SIZE
      x <- 0 until TARGET_SIZE
    } {
      val rgb   = image.getRGB(x, y)
      pixels(0)(y)(x) = ((rgb >> 16) & 0xFF) / 255.0f  // R
      pixels(1)(y)(x) = ((rgb >> 8)  & 0xFF) / 255.0f  // G
      pixels(2)(y)(x) = (rgb         & 0xFF) / 255.0f  // B
    }
    pixels
  }

  def process(imagePath: String): Option[Array[Array[Array[Float]]]] = {
    val file = new File(imagePath)
    if (file.exists()) {
      val image = ImageIO.read(file)
      val resized = resize(image)
      Some(normalize(resized))
    } else None
  }
}