
/**
  * Created by karen on 17-9-19.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object lab4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lab4").setMaster("local")
    val  sc = new SparkContext(conf)
    //val retweet = sc.textFile("HDFS:///ds410/retweet/retweetsmall.csv")
    //val retweet=sc.textFile("/home/karen/workspace/py_workspace/algorithms/410/small.csv")
    //val fields = retweet.flatMap(_.split(","))
    //val lines=fields.collect()
    //lines.foreach(println(_))
    //val has_no = fields.filter(line =>line(4) contains "no")
    //val has_no=fields.filter(_.contains("no"))
    //has_no.collect().foreach(println(_))
    //has_no.cache()
    //val total = has_no.count()
    val pattern="\\w+".r
    val retweet = sc.textFile("retweetsmall.csv")
    val fields = retweet.map(_.split("\t"))
    //val tokens=lines.map(x=>pattern.findAllIn(x.toLowerCase()).toSeq)
    //val has_no = fields.map(x=>x(2)).map(x=>pattern.findAllIn(x.toLowerCase()).toSeq)
    //.filter(_.contains("no"))
    val has_no=fields
      .map(x=>x(2))
      .filter(x=>x.contains("no")||x.contains("not")||x.contains("hate"))
      .filter(x=> !x.contains("love"))
      .filter(x=> !x.contains("like"))
      .filter(x=> !x.contains("good"))
    has_no.cache()
    has_no.collect().foreach(println(_))
    val total = has_no.count()

  }

}
