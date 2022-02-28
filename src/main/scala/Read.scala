import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, col, collect_list}

trait Read {

  val spark = SparkSession
    .builder()
    .appName("Task2Films")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  def reads(dell: String, file: String, col1: String, col2: String,
            newCol1: String, newCol2: String):sql.DataFrame = {

     spark.read
      .option("delimiter", dell)
      .csv(file)
      .select(col1, col2)
      .withColumnRenamed(col1, newCol1)
      .withColumnRenamed(col2, newCol2)
  }

  def path1(df: sql.DataFrame, film: String, col: String): String = {
    df
      .filter(film)
      .select(col)
      .collect()
      .apply(0)
      .mkString
  }

  def task (df: sql.DataFrame, colSort: String, colRes: String, colCount: String): Dataset[Row] = {
    df
      .groupBy(colSort)
      .count()
      .sort(asc(colSort))
      .withColumn(colRes, col(colCount))
      .drop(colSort, colCount)
      .agg(collect_list(colRes))
      .as(colRes)
  }
}
