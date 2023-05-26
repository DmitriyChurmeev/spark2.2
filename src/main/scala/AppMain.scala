import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object AppMain {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Films read-write")
      .master("local")
      .getOrCreate()

    val moviesSchema = StructType(Seq(
      StructField("", IntegerType),
      StructField("show_id", IntegerType),
      StructField("type", StringType),
      StructField("title", StringType),
      StructField("director", StringType),
      StructField("cast", StringType),
      StructField("country", StringType),
      StructField("date_added", StringType),
      StructField("release_year", IntegerType),
      StructField("rating", StringType),
      StructField("duration", IntegerType),
      StructField("listed_in", StringType),
      StructField("description", StringType),
      StructField("year_added", IntegerType),
      StructField("month_added", StringType),
      StructField("season_count", IntegerType),
    ))

    val filmsDataFrame = sparkSession.read
      .format("csv")
      .schema(moviesSchema)
      .option("sep", ",")
      .option("header", "true")
      .load("src/main/resources/movies_on_netflix.csv")

    filmsDataFrame.printSchema()

    filmsDataFrame.write
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/result_movies_on_netflix.parquet")
  }
}
