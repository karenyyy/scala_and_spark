import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by karen on 10/17/17.
  */
object pg {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("PageRank")
      .setMaster("local")

    val sc=new SparkContext(conf)

    val lines=sc.textFile("web-Google.txt")
    lines.partitioner.foreach(println(_))

    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 to 10) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks=contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }


  }

}
