import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Driver {

  val conf=new SparkConf().setAppName("flight-delays").setMaster("local")
  val sc=new SparkContext(conf)


  def notHeader(row: String): Boolean = {
    !row.contains("Description")
  }

  def main(args: Array[String]): Unit = {
    // Data location
    val airlinesPath="data/flight-delays/airlines.csv"
    val airportsPath="data/flight-delays/airports.csv"
    val flightsPath="data/flight-delays/flights.csv"

    val airlines=sc.textFile(airlinesPath)

    airlines.filter(x => !x.contains("Description"))
    //airlines.collect().foreach(println(_))


    airlines.filter(notHeader).take(10)
  }
}
