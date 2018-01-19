```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark data loading")
  .getOrCreate()
```


><pre>
> import org.apache.spark.sql.SparkSession
> spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@7079b1af





```scala
val df = spark.read.option("header", true)
                   .option("inferSchema", true)
                   .csv("train.csv")
```


><pre>
> df: org.apache.spark.sql.DataFrame = [Id: int, MSSubClass: int ... 79 more fields]





```scala
df
```


><pre>
> res7: org.apache.spark.sql.DataFrame = [Id: int, MSSubClass: int ... 79 more fields]





```scala
df.dtypes
```


><pre>
> res11: Array[(String, String)] = Array((Id,IntegerType), (MSSubClass,IntegerType), (MSZoning,StringType), (LotFrontage,StringType), (LotArea,IntegerType), (Street,StringType), (Alley,StringType), (LotShape,StringType), (LandContour,StringType), (Utilities,StringType), (LotConfig,StringType), (LandSlope,StringType), (Neighborhood,StringType), (Condition1,StringType), (Condition2,StringType), (BldgType,StringType), (HouseStyle,StringType), (OverallQual,IntegerType), (OverallCond,IntegerType), (YearBuilt,IntegerType), (YearRemodAdd,IntegerType), (RoofStyle,StringType), (RoofMatl,StringType), (Exterior1st,StringType), (Exterior2nd,StringType), (MasVnrType,StringType), (MasVnrArea,StringType), (ExterQual,StringType), (ExterCond,StringType), (Foundation,StringType), (BsmtQual,StringType), (Bs...





```scala
df.select("Id", "SalePrice")
```


><pre>
> res15: org.apache.spark.sql.DataFrame = [Id: int, SalePrice: int]





```scala
df.filter(df("SalePrice") > 20000).select("Id", "SalePrice")
```


><pre>
> res13: org.apache.spark.sql.DataFrame = [Id: int, SalePrice: int]





```scala
val avgs = df.groupBy("YearBuilt").agg(avg("SalePrice"))
LineChart(avgs.sort("YearBuilt"))
```


><pre>
> avgs: org.apache.spark.sql.DataFrame = [YearBuilt: int, avg(SalePrice): double]
> res15: notebook.front.widgets.charts.LineChart[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = <LineChart widget>





```scala
val richestNeighborhood = df.groupBy("Neighborhood").agg(sum("SalePrice"))
PieChart(richestNeighborhood)
```


><pre>
> richestNeighborhood: org.apache.spark.sql.DataFrame = [Neighborhood: string, sum(SalePrice): bigint]
> res17: notebook.front.widgets.charts.PieChart[org.apache.spark.sql.DataFrame] = <PieChart widget>





```scala
val radarData = df.select("Id", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath")
RadarChart(radarData.take(2), labelField=Some("Id"))
```


><pre>
> radarData: org.apache.spark.sql.DataFrame = [Id: int, BsmtFullBath: int ... 3 more fields]
> res19: notebook.front.widgets.charts.RadarChart[Array[org.apache.spark.sql.Row]] = <RadarChart widget>
