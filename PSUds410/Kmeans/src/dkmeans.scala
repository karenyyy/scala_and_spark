import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import scala.io.Source
import scala.util.Try

object GetData extends java.io.Serializable {
   def convert(x: Array[String]) = {
     val parsed = (10 to 13).map(y => x(y).toDouble).toVector
     parsed
   }   

   def getData(sc: SparkContext, name: String="/ds410/taxi") = {
      val tmpdata = sc.textFile(name).map(x => x.trim().split(","))
      val points = tmpdata.map{x => Try(convert(x))}
      val goodpoints = points.filter(_.isSuccess).map(_.get).persist()
      goodpoints
   }
}

object Cluster extends java.io.Serializable {

  def distance(point1: Vector[Double], point2: Vector[Double]) = {
     point1.zip(point2).map(x => Math.pow(x._1 -  x._2, 2)).reduce(_ + _)
  }

  //return id of cluster nearest to the point
  def getClusterID(centers: Vector[Vector[Double]], point: Vector[Double]) = {
     //map centers to distance from the point, convert it to (element, index) tuples
     // then find min (which returns (element, index)) then extract the index
     centers.map(x => distance(x, point)).zipWithIndex.min._2
  }

  //initial cluster centers are the first k data points
  def initClusters(k: Int, data: RDD[Vector[Double]]) = {
    val centers = data.take(k).toVector
    centers
  }

  def addToLeft(lvalue: Array[Double], value: Seq[Double]): Unit = {
    value.zipWithIndex.foreach{z => lvalue(z._2) += z._1}
  }  

  def updateCenters(centers: Vector[Vector[Double]], data: RDD[Vector[Double]]) = {
     //as we go through the data, for each cluster, we store the running sum of points
     // assigned to it and the number of points assigned to it so far
     //Array.tabulate creates an array of 0's of same size as a cluster center
     // vector.tabulate creates k of these
     val runningSum: Vector[Array[Double]] = Vector.tabulate(centers.size){i => Array.tabulate(centers(i).size){j => 0.0}}
     val runningCount = Array.tabulate(centers.size){i => 0.0}
     val (theSum, theCount) = data.aggregate((runningSum, runningCount))(
        //aggregate within a worker
        (accum, point) => {
           val cl = getClusterID(centers, point)
           addToLeft(accum._1(cl), point)
           accum._2(cl) += 1
           accum
        }
        ,
        //aggregate across workers
        (accum, localSummary) => {
           accum._1.zip(localSummary._1).foreach{x => addToLeft(x._1, x._2)}
           addToLeft(accum._2, localSummary._2)
           accum
        }

     )
     theSum.zip(theCount).map{z => z._1.map(value => value/z._2).toVector}
  }


  //k = number of clusters, iter = number of iterations of the algorithm
  def kmeans(k: Int, iter: Int, sc: SparkContext) = {
     val data = GetData.getData(sc)
     val initCenters = initClusters(k, data)
     val finalCenters = (1 to iter).foldLeft(initCenters){(centers, index) => updateCenters(centers, data)}
     finalCenters
  }


}
