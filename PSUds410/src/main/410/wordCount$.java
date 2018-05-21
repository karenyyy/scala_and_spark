import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    val pattern="(http.*)?[A-Za-z]+(\')?[A-Za-z]+".r

    val conf = new SparkConf().setAppName("lab4").setMaster("local")
    val  sc = new SparkContext(conf)
    val textFile = sc.textFile("retweetsmall.csv") //local or hdfs://
    val tokens = textFile.map(line => line.split("\t"))
      .map(_(2))
      .flatMap(x=>pattern.findAllIn(x.toLowerCase()).toSeq)
      .map((_, 1))
      .reduceByKey(_ + _)
      .map{_.swap}
      .sortByKey()
      .take(100)
      .map{_.swap}



    val pattern2="\\s+school[\\W]?\\s+".r
    val text=sc.parallelize(List("Ikadka aknsdakmjbdja ajs schooll school. jdqjad schoolmates, kdjkande school schoolbus."))
    val res=text.flatMap(x=>pattern2.findAllIn(x).toSeq).foreach(println(_))
  }



}
