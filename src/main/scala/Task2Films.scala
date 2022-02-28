import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

import scala.language.implicitConversions

object Task2Films extends App with Read {

  val itemDF = reads("|", "u.item", "_c0", "_c1",
   "filmID", "title")

  itemDF.createOrReplaceTempView("list_films")

  val dataDF = reads("\t", "u.data", "_c1", "_c2",
   "filmID", "rating")

  dataDF.createOrReplaceTempView("rating_films")

  val joinDF = spark.sql("SELECT list_films.filmID, list_films.title, rating_films.rating " +
      "FROM rating_films INNER JOIN list_films ON rating_films.filmID = list_films.filmID")
    .withColumn("filmID",col("filmID").cast(IntegerType))

  val path = path1(itemDF,"filmID == 32","title")

  val taskFilm = joinDF
    .where("filmID == 32")

  val task1 = task(taskFilm, "rating", path, "count")

  val task2 = task(joinDF, "rating", "hist_all", "count")

  val result: Unit = task1.join(task2)
    .write
    .mode("ignore")
    .json("result")
}




