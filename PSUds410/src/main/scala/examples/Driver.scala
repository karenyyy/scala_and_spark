
import java.io.PrintWriter
import java.io.File
import java.util.Arrays

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions
import scala.io.Source
import org.apache.spark.SparkConf


object Driver {
  private final val SPARK_MASTER="local"
  private final val APP_NAME="driver"
  

  def main(args: Array[String]): Unit = {
    // configure sparkcontext
    val conf=new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME)
    val sc=new SparkContext(conf)

    
    // the parse lines and extract tokens;
    val lines=sc.textFile("retweetsmall.csv")
    val cleanedLines=lines
      .map(_.split("\t"))
      //.map ( x => if (x.length > 3) ""; else x(2))
      .map(x=>x(2))
      .map(_.split("[ .,?!:\"]"))
      //.map( line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    val words=cleanedLines.flatMap(x=>x)
    //
    words.collect().foreach(println(_))
    // Filter out Hash Tags
    //val hashTags = words.filter(x => x.length() > 0 && x(0) == '#');
    
    // Get Hash Tag Frequency
    //val hashKeyValue = hashTags.map(x => (x, 1));
    //val hashFreq = hashKeyValue.reduceByKey((x,y) => x+y);

    // Sort Hash Tag Frequency, Get Top 100
    //val top100 = hashFreq.collect.toSeq.sortWith(_._2 > _._2).take(100);

    // Write Output File
    //val writer = new PrintWriter(new File("output.txt"));
    //top100.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"));
    //writer.close();
    
        
    
    
    
  }
}