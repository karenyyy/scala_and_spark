import scala.io.Source
import scala.util.Try

object Cluster extends App {

  def getData(name:String="smalldata.csv"): Vector[Vector[Double]] = {
    val parts = Source.fromFile(name).getLines().map(x => x.trim().split(","))
    val points = parts.map(x => Try(Vector(x(10).toDouble, x(11).toDouble, x(12).toDouble, x(13).toDouble)))
    val goodpoints = points.filter(_.isSuccess).map(_.get).toVector
    goodpoints
  }

  def distance(point1: Vector[Double], point2: Vector[Double]) = {
     point1.zip(point2)
       .map(x => Math.pow(x._1 - x._2, 2))
       .reduce(_+_)
  }

  // randonly pick centroids
  def initClusters(k:Int, data: Vector[Vector[Double]]) = {
    val centers = Vector.tabulate(k){i => data(i)}
    centers
  }

  def getClusterID(centers: Vector[Vector[Double]], point: Vector[Double]) = {
    // get the index with the smallest distance
    centers.map(x => distance(x, point)).zipWithIndex.min._2
  }

  def addToLeft(accum: Array[Double], value: Vector[Double]):Unit = {
    value.zipWithIndex.foreach(z => accum(z._2) += z._1)
  }

  def updateCenters(centers: Vector[Vector[Double]], data: Vector[Vector[Double]]) =  {
    // cluster_i(all points inside the cluster)
    // initialize each cluster
    val runningSum = Vector.tabulate(centers.size){i => Array.tabulate(centers(i).size){j => 0.0}}
    val runningCount = Array.tabulate(centers.size){i => 0}
    data.foreach{point => {
      val cl = getClusterID(centers, point)
      addToLeft(runningSum(cl), point)  // sum of center(i).size elements inside cluster cl
      runningCount(cl) += 1
    }}

    // cluster_i(all points inside the cluster):number of elements in the cluster
    // get average of the new cluster and update it as the new centroid
    runningSum.zip(runningCount).map(z => z._1.map(value => value/z._2).toVector)
  }



  def kmeans(k: Int, iter: Int) = {
    val data = getData()
    val initCenters = initClusters(k,data)
    val finalCenters = (1 to iter).foldLeft(initCenters)((centers, _) => updateCenters(centers,data))
    finalCenters
  }

  override def main(args: Array[String]): Unit = {
    println(kmeans(5, 10))
  }
}
