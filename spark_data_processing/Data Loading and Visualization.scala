
object Cells {
  import org.apache.spark.sql.SparkSession
  
  val spark = SparkSession
    .builder()
    .appName("Spark data loading")
    .getOrCreate()

  /* ... new cell ... */

  val df = spark.read.option("header", true)
                     .option("inferSchema", true)
                     .csv("train.csv")

  /* ... new cell ... */

  df

  /* ... new cell ... */

  df.dtypes

  /* ... new cell ... */

  df.select("Id", "SalePrice")

  /* ... new cell ... */

  df.filter(df("SalePrice") > 20000).select("Id", "SalePrice")

  /* ... new cell ... */

  val avgs = df.groupBy("YearBuilt").agg(avg("SalePrice"))
  LineChart(avgs.sort("YearBuilt"))

  /* ... new cell ... */

  val richestNeighborhood = df.groupBy("Neighborhood").agg(sum("SalePrice"))
  PieChart(richestNeighborhood)

  /* ... new cell ... */

  val radarData = df.select("Id", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath")
  RadarChart(radarData.take(2), labelField=Some("Id"))

  /* ... new cell ... */
}
                  