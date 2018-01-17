import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Created by karen on 10/19/17.
  */

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

object intro extends Serializable{
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setAppName("intro").setMaster("local")
    val sc=new SparkContext(conf)
    //sc.textFile("donation/block_1.csv").collect().take(10).foreach(println(_))


    val sp=SparkSession.builder().appName("intro").getOrCreate()
    val view=sp.read.csv("donation/block_1.csv")
    view.show()
    view.printSchema()

    val parsed = sp.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("donation/block_1.csv")
    parsed.show()
    parsed.printSchema()
    parsed.count()
    parsed.cache()
    parsed.groupBy("is_match")
      .count()
      .orderBy("count")
      .show()


    parsed.createOrReplaceTempView("donation/block_1")
    sp.sql(
      """
        |SELECT is_match, COUNT(*) cnt
        |FROM donation/block_1
        |GROUP BY is_match
        |ORDER BY cnt DESC
      """.stripMargin).show()


  }

}
