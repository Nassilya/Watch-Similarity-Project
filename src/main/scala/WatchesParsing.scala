import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object WatchesParsing {
  private val logger = logger_config.logger
  private val workingDir = Paths.get("").toAbsolutePath.normalize()
  private val workspaceRoot = workingDir.getParent match {
    case null => workingDir
    case parent => parent
  }

  def findMetadata(root: Path): Path = {
    val stream = Files.walk(root)
    try {
      stream
        .filter(path => Files.isRegularFile(path) && path.getFileName.toString.equalsIgnoreCase("metadata.csv"))
        .findFirst()
        .orElseThrow(() => new IllegalArgumentException(s"metadata.csv introuvable dans $root"))
    } finally {
      stream.close()
    }
  }

  def findImageDir(root: Path): Path = {
    val candidates = Seq(
      root.resolve("images"),
      root.resolve("watches").resolve("images")
    ) ++ Option(root.getParent).map(_.resolve("images"))

    candidates.find(Files.isDirectory(_)).getOrElse {
      val stream = Files.walk(root)
      try {
        val found = stream
          .filter(path => Files.isDirectory(path) && path.getFileName.toString.equalsIgnoreCase("images"))
          .findFirst()
        if (found.isPresent) found.get()
        else throw new IllegalArgumentException(s"Dossier images introuvable (ROOT=$root)")
      } finally {
        stream.close()
      }
    }
  }

  def resolveInputRoot(args: Array[String]): Path = {
    if (args.nonEmpty) {
      Paths.get(args(0)).toAbsolutePath.normalize()
    } else {
      val candidates = Seq(
        workingDir.resolve("watches_data"),
        workingDir.resolve("src").resolve("main").resolve("scala").resolve("watches_data"),
        workspaceRoot.resolve("watches_data"),
        workspaceRoot.resolve("data").resolve("watches_data"),
        workspaceRoot.resolve("dataset").resolve("watches_data"),
        workspaceRoot.resolve("Parsing").resolve("src").resolve("main").resolve("scala").resolve("watches_data")
      ).distinct

      candidates.find(Files.isDirectory(_)).getOrElse {
        throw new IllegalArgumentException(
          s"""Dossier de donnees introuvable.
             |Chemins cherches:
             | - ${workingDir.resolve("watches_data")}
             | - ${workingDir.resolve("src").resolve("main").resolve("scala").resolve("watches_data")}
             | - ${workspaceRoot.resolve("watches_data")}
             | - ${workspaceRoot.resolve("Parsing").resolve("src").resolve("main").resolve("scala").resolve("watches_data")}
             |
             |Place le dataset dans l'un de ces dossiers ou lance:
             |sbt "run C:/chemin/vers/watches_data"
             |""".stripMargin
        )
      }
    }
  }

  def resolveOutputPath(args: Array[String]): String = {
    if (args.length > 1) args(1)
    else workingDir.resolve("data").resolve("watches_parsed.parquet").toString
  }

  def buildMetadata(spark: SparkSession, csvPath: Path): DataFrame = {
    spark.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .csv(csvPath.toString)
      .withColumn("image_name", trim(col("image_name")))
      .withColumn("label", initcap(trim(col("brand"))))
      .filter(col("image_name").isNotNull && length(col("image_name")) > 0)
      .filter(col("label").isNotNull && length(col("label")) > 0)
      .select("image_name", "label")
      .dropDuplicates("image_name")
  }

  def buildImages(spark: SparkSession, imageDir: Path): DataFrame = {
    spark.read
      .format("binaryFile")
      .option("recursiveFileLookup", "true")
      .load(imageDir.toString)
      .select("path", "content")
      .filter(lower(col("path")).rlike("\\.(jpg|jpeg|png)$"))
      .withColumn("image_name", regexp_extract(col("path"), "([^/\\\\]+)$", 1))
      .withColumn("image_name", trim(col("image_name")))
      .dropDuplicates("image_name")
  }

  def assignSplit(df: DataFrame): DataFrame = {
    val bucket = pmod(xxhash64(col("image_name")), lit(100))

    df.withColumn("bucket", bucket)
      .withColumn(
        "split",
        when(col("bucket") < 80, "train")
          .when(col("bucket") < 90, "val")
          .otherwise("test")
      )
      .drop("bucket")
  }

  def main(args: Array[String]): Unit = {
    val rootPath = resolveInputRoot(args)
    val outputPath = resolveOutputPath(args)

    val csvPath = findMetadata(rootPath)
    val imageDir = findImageDir(csvPath.getParent)

    val spark = SparkSession.builder()
      .appName("Watches-Parsing")
      .master("local[*]")
      .getOrCreate()

    logger.info(s"DATA_ROOT : $rootPath")
    logger.info(s"CSV_PATH  : $csvPath")
    logger.info(s"IMG_DIR   : $imageDir")
    logger.info(s"OUTPUT    : $outputPath")

    val meta = buildMetadata(spark, csvPath)
    val imgs = buildImages(spark, imageDir)
    val missingImages = meta.join(imgs.select("image_name"), Seq("image_name"), "left_anti").count()

    val parsed = assignSplit(
      meta.join(imgs, Seq("image_name"), "inner")
    )

    val rowCount = parsed.count()
    val numClasses = parsed.select("label").distinct().count()

    logger.info(s"Rows retenues : $rowCount")
    logger.info(s"Classes       : $numClasses")
    logger.info(s"Images ignorees faute de fichier: $missingImages")
    parsed.groupBy("split").count().orderBy("split").show(false)
    parsed.groupBy("label").count().orderBy(desc("count")).show(10, truncate = false)

    parsed
      .select("image_name", "path", "label", "split", "content")
      .write
      .mode("overwrite")
      .parquet(outputPath)

    logger.info(s"Wrote: $outputPath")
    spark.stop()
  }
}
