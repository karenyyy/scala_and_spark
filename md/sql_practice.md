```scala
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
```


><pre>
> import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
> import org.apache.spark.sql.functions._





```scala
case class MatchData(
                      id_1: Int,
                      id_2: Int,
                      cmp_fname_c1: Option[Double],
                      cmp_fname_c2: Option[Double],
                      cmp_lname_c1: Option[Double],
                      cmp_lname_c2: Option[Double],
                      cmp_sex: Option[Int],
                      cmp_bd: Option[Int],
                      cmp_bm: Option[Int],
                      cmp_by: Option[Int],
                      cmp_plz: Option[Int],
                      is_match: Boolean
                    )

```


><pre>
> defined class MatchData





```scala
val spark = SparkSession.builder
      .appName("sql1")
      .getOrCreate

import spark.implicits._
val preview = spark.read.csv("data/block_5.csv")
preview.show()
preview.printSchema()
```


><pre>
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> |  _c0|  _c1|         _c2|         _c3|         _c4|         _c5|    _c6|   _c7|   _c8|   _c9|   _c10|    _c11|
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> | id_1| id_2|cmp_fname_c1|cmp_fname_c2|cmp_lname_c1|cmp_lname_c2|cmp_sex|cmp_bd|cmp_bm|cmp_by|cmp_plz|is_match|
> |31641|62703|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |27816|46246|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |  980| 2651|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> | 6514| 8780|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> | 5532|14374|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |25763|61342|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |59655|59657|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |23800|32179|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |33568|38196|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |95679|95680|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |23729|60864|           1|           1|           1|           1|      1|     1|     1|     1|      1|    TRUE|
> |21573|36660|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |26251|40278|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |77101|77106|           1|           ?|           1|           1|      1|     1|     1|     1|      1|    TRUE|
> |44816|87744|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |17156|49320|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |20166|37894|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |28894|34446|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> |95236|95834|           1|           ?|           1|           ?|      1|     1|     1|     1|      1|    TRUE|
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> only showing top 20 rows
> 
> root
>  |-- _c0: string (nullable = true)
>  |-- _c1: string (nullable = true)
>  |-- _c2: string (nullable = true)
>  |-- _c3: string (nullable = true)
>  |-- _c4: string (nullable = true)
>  |-- _c5: string (nullable = true)
>  |-- _c6: string (nullable = true)
>  |-- _c7: string (nullable = true)
>  |-- _c8: string (nullable = true)
>  |-- _c9: string (nullable = true)
>  |-- _c10: string (nullable = true)
>  |-- _c11: string (nullable = true)
> 
> spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@4a30307e
> import spark.implicits._
> preview: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 10 more fields]





```scala
val parsed = spark.read
                      .option("header", "true")
                      .option("nullValue", "?")
                      .option("inferSchema", "true")
                      .csv("data/block_5.csv")
```


><pre>
> parsed: org.apache.spark.sql.DataFrame = [id_1: int, id_2: int ... 10 more fields]





```scala
parsed.show()
parsed.printSchema()
```


><pre>
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> | id_1| id_2|cmp_fname_c1|cmp_fname_c2|cmp_lname_c1|cmp_lname_c2|cmp_sex|cmp_bd|cmp_bm|cmp_by|cmp_plz|is_match|
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> |31641|62703|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |27816|46246|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |  980| 2651|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> | 6514| 8780|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> | 5532|14374|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |25763|61342|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |59655|59657|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |23800|32179|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |33568|38196|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |95679|95680|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |23729|60864|         1.0|         1.0|         1.0|         1.0|      1|     1|     1|     1|      1|    true|
> |21573|36660|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |26251|40278|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |77101|77106|         1.0|        null|         1.0|         1.0|      1|     1|     1|     1|      1|    true|
> |44816|87744|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |17156|49320|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |20166|37894|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |28894|34446|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |95236|95834|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> |37609|68906|         1.0|        null|         1.0|        null|      1|     1|     1|     1|      1|    true|
> +-----+-----+------------+------------+------------+------------+-------+------+------+------+-------+--------+
> only showing top 20 rows
> 
> root
>  |-- id_1: integer (nullable = true)
>  |-- id_2: integer (nullable = true)
>  |-- cmp_fname_c1: double (nullable = true)
>  |-- cmp_fname_c2: double (nullable = true)
>  |-- cmp_lname_c1: double (nullable = true)
>  |-- cmp_lname_c2: double (nullable = true)
>  |-- cmp_sex: integer (nullable = true)
>  |-- cmp_bd: integer (nullable = true)
>  |-- cmp_bm: integer (nullable = true)
>  |-- cmp_by: integer (nullable = true)
>  |-- cmp_plz: integer (nullable = true)
>  |-- is_match: boolean (nullable = true)





```scala
parsed.count()
parsed.persist()
```


><pre>
> res114: parsed.type = [id_1: int, id_2: int ... 10 more fields]





```scala
parsed.groupBy("is_match")
          .count()
          .orderBy($"count".desc)
          .show()
```


><pre>
> +--------+------+
> |is_match| count|
> +--------+------+
> |   false|572820|
> |    true|  2094|
> +--------+------+





```scala
parsed.createOrReplaceTempView("linkage")
```




```scala
sqlContext.sql("""
      SELECT is_match, COUNT(*) count_
      FROM linkage
      GROUP BY is_match
      ORDER BY count_ DESC
    """).show()
```


><pre>
> +--------+------+
> |is_match|count_|
> +--------+------+
> |   false|572820|
> |    true|  2094|
> +--------+------+





```scala
val summary=parsed.describe() // same as in R, summar description: standard 5 factors;
summary.show()
```


><pre>
> +-------+------------------+-----------------+------------------+------------------+-------------------+-------------------+-------------------+------------------+-------------------+------------------+--------------------+
> |summary|              id_1|             id_2|      cmp_fname_c1|      cmp_fname_c2|       cmp_lname_c1|       cmp_lname_c2|            cmp_sex|            cmp_bd|             cmp_bm|            cmp_by|             cmp_plz|
> +-------+------------------+-----------------+------------------+------------------+-------------------+-------------------+-------------------+------------------+-------------------+------------------+--------------------+
> |  count|            574914|           574914|            574802|             10414|             574914|                257|             574914|            574824|             574824|            574824|              573639|
> |   mean|33339.355670587254|66606.91516122411|0.7135298994861159|0.9033751497526223|  0.315423189875679| 0.3282131508785206|  0.954506239194036|0.2246478922244026|0.48887137628213156|0.2223550164920045|0.005567961732030074|
> | stddev|23643.556028111416|23597.93209267407|0.3885410750690404| 0.266907055420433|0.33374256832926946|0.37527229651401645|0.20838463010802555|0.4173505957353688| 0.4998765732010075|0.4158287675764643|  0.0744108136398136|
> |    min|                 2|               44|               0.0|               0.0|                0.0|                0.0|                  0|                 0|                  0|                 0|                   0|
> |    max|             99902|           100000|               1.0|               1.0|                1.0|                1.0|                  1|                 1|                  1|                 1|                   1|
> +-------+------------------+-----------------+------------------+------------------+-------------------+-------------------+-------------------+------------------+-------------------+------------------+--------------------+
> 
> summary: org.apache.spark.sql.DataFrame = [summary: string, id_1: string ... 10 more fields]





```scala
summary.select("summary", "id_1", "cmp_lname_c1")
        .show()
```


><pre>
> +-------+------------------+-------------------+
> |summary|              id_1|       cmp_lname_c1|
> +-------+------------------+-------------------+
> |  count|            574914|             574914|
> |   mean|33339.355670587254|  0.315423189875679|
> | stddev|23643.556028111416|0.33374256832926946|
> |    min|                 2|                0.0|
> |    max|             99902|                1.0|
> +-------+------------------+-------------------+





```scala
parsed.select("is_match")
```


><pre>
> res156: org.apache.spark.sql.DataFrame = [is_match: boolean]





```scala
val matches=parsed.where("is_match=true")
val del=parsed.filter($"is_match"=== false)
matches.describe()
```


><pre>
> matches: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id_1: int, id_2: int ... 10 more fields]
> del: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [id_1: int, id_2: int ... 10 more fields]
> res158: org.apache.spark.sql.DataFrame = [summary: string, id_1: string ... 10 more fields]





```scala
del.describe()
```


><pre>
> res160: org.apache.spark.sql.DataFrame = [summary: string, id_1: string ... 10 more fields]





```scala
val matchSummary = matches.describe()
val delSummary = del.describe()

```



><pre>
> matchSummary: org.apache.spark.sql.DataFrame = [summary: string, id_1: string ... 10 more fields]
> delSummary: org.apache.spark.sql.DataFrame = [summary: string, id_1: string ... 10 more fields]





```scala
def longForm(desc: DataFrame): DataFrame = {
    import desc.sparkSession.implicits._ // For toDF RDD -> DataFrame conversion
    val schema = desc.schema
    desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => (metric, schema(i).name, row.getString(i).toDouble))
    })
      .toDF("metric", "field", "value")
  }

def pivotSummary(desc: DataFrame): DataFrame = {
    val lf = longForm(desc)
    lf.groupBy("field").
      pivot("metric", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))
  }
```



><pre>
> longForm: (desc: org.apache.spark.sql.DataFrame)org.apache.spark.sql.DataFrame
> pivotSummary: (desc: org.apache.spark.sql.DataFrame)org.apache.spark.sql.DataFrame





```scala
val matchSummaryT = pivotSummary(matchSummary)
val delSummaryT = pivotSummary(delSummary)

matchSummaryT
```



><pre>
> matchSummaryT: org.apache.spark.sql.DataFrame = [field: string, count: double ... 4 more fields]
> delSummaryT: org.apache.spark.sql.DataFrame = [field: string, count: double ... 4 more fields]
> res164: org.apache.spark.sql.DataFrame = [field: string, count: double ... 4 more fields]





```scala
delSummaryT
```



><pre>
> res166: org.apache.spark.sql.DataFrame = [field: string, count: double ... 4 more fields]





```scala
matchSummaryT.createOrReplaceTempView("match_desc")
delSummaryT.createOrReplaceTempView("del_desc")
spark.sql("""
      SELECT a.field, a.count + b.count total, a.mean - b.mean delta
      FROM match_desc a INNER JOIN del_desc b ON a.field = b.field
      ORDER BY delta DESC, total DESC
    """).show()
```



><pre>
> +------------+--------+-------------------+
> |       field|   total|              delta|
> +------------+--------+-------------------+
> |        id_1|574914.0| 1532.4758025618794|
> |     cmp_plz|573639.0| 0.9568124441677914|
> |cmp_lname_c2|   257.0|  0.788875728404714|
> |      cmp_by|574824.0| 0.7771301216551673|
> |      cmp_bd|574824.0| 0.7757879250040416|
> |cmp_lname_c1|574914.0| 0.6829795637937397|
> |      cmp_bm|574824.0| 0.5105988571868638|
> |cmp_fname_c1|574802.0|  0.284498689450304|
> |cmp_fname_c2| 10414.0|0.08809501556461785|
> |     cmp_sex|574914.0|0.03128104769756701|
> |        id_2|574914.0|-15148.350499184206|
> +------------+--------+-------------------+





```scala
val matchData = parsed.as[MatchData]
```



><pre>
> matchData: org.apache.spark.sql.Dataset[MatchData] = [id_1: int, id_2: int ... 10 more fields]





```scala
def crossTabs(scored: DataFrame, t: Double): DataFrame = {
    scored.
      selectExpr(s"score >= $t as above", "is_match").
      groupBy("above").
      pivot("is_match", Seq("true", "false")).
      count()
  }

  case class Score(value: Double) {
    def +(oi: Option[Int]) = {
      Score(value + oi.getOrElse(0))
    }
  }

  def scoreMatchData(md: MatchData): Double = {
    (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
      md.cmp_by + md.cmp_bd + md.cmp_bm).value
  }
```



><pre>
> crossTabs: (scored: org.apache.spark.sql.DataFrame, t: Double)org.apache.spark.sql.DataFrame
> defined class Score
> scoreMatchData: (md: MatchData)Double





```scala
val scored = matchData.map { md =>
      (scoreMatchData(md), md.is_match)
    }.toDF("score", "is_match")
    crossTabs(scored, 4.0).show()
scored
```



><pre>
> +-----+----+------+
> |above|true| false|
> +-----+----+------+
> | true|2087|    69|
> |false|   7|572751|
> +-----+----+------+
> 
> scored: org.apache.spark.sql.DataFrame = [score: double, is_match: boolean]
> res172: org.apache.spark.sql.DataFrame = [score: double, is_match: boolean]
