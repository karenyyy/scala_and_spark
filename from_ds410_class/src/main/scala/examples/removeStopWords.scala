import org.apache.spark._
import org.apache.spark.rdd.RDD;

object removeStopWords {
  def processLine(s: String, stopWords: Set[String]): Seq[String] = {
    s.replaceAll("[^a-zA-Z ]", " ")
      .toLowerCase()
      .split(" ")
      .filter(!stopWords.contains(_)).toSeq
}
  def main(args: Array[String]): Unit = {
    if (args.length<2) {
      println("Usage:[sparkmaster][inputfile]")
    }
    val master=args(0);
    val inputFile=args(1);
    
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"));
    val input = sc.textFile(inputFile);
    
    
    val stopWords=Set("I", "your","me", "and");
    //val result=processLine(input, stopWords);
    val ans=input.flatMap ( x => x.split("\\W+") ).filter(!stopWords.contains(_))
    println(ans);
    
  }
}