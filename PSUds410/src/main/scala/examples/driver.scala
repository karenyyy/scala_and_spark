import java.io.PrintWriter
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext



object driver {
  val conf=new SparkConf(false)
    .setMaster("local")
    .setAppName("driver");
  val sc=new SparkContext(conf)
  val data=sc.textFile("hdfs:/ds410/tweets/nyc-twitter-data-2013.csv")
  val lines=data.map(line=>line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
  val users=lines.map(cs=>(if (cs.size<7) "";else cs(2)))
  val counts=users.map(user=>(user,1))
    .reduceByKey(_+_)
  val sortedCounts=counts.sortBy(_._2, false)
  
  val top50=sortedCounts.take(50)
  val writer=new PrintWriter(new File("output.txt"))
  top50.foreach(x=>writer.write(x._1+"\t"+x._2+"\n"))
  writer.close()
    }