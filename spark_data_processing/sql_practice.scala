
object Cells {
  import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
  import org.apache.spark.sql.functions._

  /* ... new cell ... */

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

  /* ... new cell ... */

  val spark = SparkSession.builder
        .appName("sql1")
        .getOrCreate
  
  import spark.implicits._
  val preview = spark.read.csv("data/block_5.csv")
  preview.show()
  preview.printSchema()

  /* ... new cell ... */

  val parsed = spark.read
                        .option("header", "true")
                        .option("nullValue", "?")
                        .option("inferSchema", "true")
                        .csv("data/block_5.csv")

  /* ... new cell ... */

  parsed.show()
  parsed.printSchema()

  /* ... new cell ... */

  parsed.count()
  parsed.persist()

  /* ... new cell ... */

  parsed.groupBy("is_match")
            .count()
            .orderBy($"count".desc)
            .show()

  /* ... new cell ... */

   parsed.toDF().registerTempTable("linkage") // table name: linkage

  /* ... new cell ... */

  parsed.createOrReplaceTempView("linkage")

  /* ... new cell ... */

  sqlContext.sql("""
        SELECT is_match, COUNT(*) count_
        FROM linkage
        GROUP BY is_match
        ORDER BY count_ DESC
      """).show()

  /* ... new cell ... */

  val summary=parsed.describe() // same as in R, summar description: standard 5 factors;
  summary.show()

  /* ... new cell ... */

  summary.select("summary", "id_1", "cmp_lname_c1")
          .show()

  /* ... new cell ... */

  parsed.select("is_match")

  /* ... new cell ... */

  val matches=parsed.where("is_match=true")
  val del=parsed.filter($"is_match"=== false)
  matches.describe()

  /* ... new cell ... */

  del.describe()

  /* ... new cell ... */

  val matchSummary = matches.describe()
  val delSummary = del.describe()

  /* ... new cell ... */

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

  /* ... new cell ... */

  val matchSummaryT = pivotSummary(matchSummary)
  val delSummaryT = pivotSummary(delSummary)
  
  matchSummaryT

  /* ... new cell ... */

  delSummaryT

  /* ... new cell ... */

  matchSummaryT.createOrReplaceTempView("match_desc")
  delSummaryT.createOrReplaceTempView("del_desc")
  spark.sql("""
        SELECT a.field, a.count + b.count total, a.mean - b.mean delta
        FROM match_desc a INNER JOIN del_desc b ON a.field = b.field
        ORDER BY delta DESC, total DESC
      """).show()

  /* ... new cell ... */

  val matchData = parsed.as[MatchData]

  /* ... new cell ... */

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

  /* ... new cell ... */

  val scored = matchData.map { md =>
        (scoreMatchData(md), md.is_match)
      }.toDF("score", "is_match")
      crossTabs(scored, 4.0).show()
  scored

  /* ... new cell ... */
}
                  