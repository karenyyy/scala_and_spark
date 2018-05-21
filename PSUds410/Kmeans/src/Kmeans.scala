import scala.util.Try
import java.io.PrintWriter
import java.io.File
import org.apache.spark.sql.functions._

class Kmeans (val k:Int, val f:Int) extends java.io.Serializable{


  val nb_cluster:Int = k
  val nb_feature:Int = f
  var centers:Array[(Int, List[Double])] = _

  def initialize(samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
    val tmp = samples.takeSample(false, nb_cluster).map(c => c._2)
    val tmp1 = (0 to (nb_cluster-1)).toArray
    val tmp2 = tmp1.zip(tmp)
    val clusters = tmp2.map(c => (c._1, c._2.toList))
    return clusters
  }

  def Distance(a:Array[Double], b:List[Double]) : Double = {
    assert(a.length == b.length, "Distance(): features dim does not match.")
    var dist = 0.0
    for (i <- 0 to a.length-1) {
      dist = dist + math.pow(a(i) - b(i), 2)
    }
    return math.sqrt(dist)
  }

  def dist_step(c:Array[(Int, List[Double])], samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
    // Broadcast Cluster Centroids
    val clusters = sc.broadcast(c)

    // Compute Distances
    val dist = samples.flatMap{ case(sampleID, sample) => clusters.value.map{ case (clusterID, cluster) => (sampleID, (clusterID, Distance(sample, cluster))) }}

    // Map New Labels
    val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1)) // oldID,newID

    // Join Samples and to Cluster ID & Remap Order
    val clusterKey = samples.join(labels).map(x => (x._2._2, (x._1, x._2._1))) // newID, (oldID, data)

    // Compute New Cluster Means
    var new_clusters = clusterKey.combineByKey(
      (v) => (v._2, 1),   // newID, ((oldID, data), 1)
      (acc:(Array[Double], Int), v) => (acc._1.zip(v._2).map(x => x._1 + x._2), acc._2 + 1),
      (acc1:(Array[Double], Int), acc2:(Array[Double], Int)) => (acc1._1.zip(acc2._1).map(x => x._1 + x._2), acc1._2 + acc2._2)
    ).map{ case (k, v) => (k, v._1.map(x => x / v._2.toDouble).toList) }.collect()

    return new_clusters
  }

  def for_step(c:Array[(Int, List[Double])], samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
    val clusters = Demo.sc.broadcast(c)
    val dist = samples.flatMap{ case(sampleID, sample) => clusters.value.map{
      case (clusterID, cluster) => (sampleID, (clusterID, Distance(sample, cluster)))
    }
    }
    val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1))
    var  new_clusters = Array.ofDim[(Int, Array[Double])](nb_cluster)
    for (i <- 0 to nb_cluster-1) {
      val sample_in_cluster = samples.join(labels.filter(i==_._2))
      val total_number = sample_in_cluster.count
      if (total_number != 0) {
        var tmp = sample_in_cluster.map(sample => sample._2._1).reduce((a, b) => a.zip(b).map{ case (x, y) => x + y })
        tmp = tmp.map( a => a/total_number.toDouble)
        new_clusters(i) = (i, tmp)
      }
      else {
        new_clusters(i) = (i, samples.takeSample(false, 1)(0)._2)
      }
    }
    val new_clusters_list = new_clusters.map(s => (s._1, s._2.toList))
    return new_clusters_list
  }

  def dist_run(samples:org.apache.spark.rdd.RDD[(Long, Array[Double])], max_iter:Int) : Double = {
    var i:Int = 0
    val t0 = System.nanoTime()
    centers = initialize(samples)
    while(i < max_iter) {
      centers = dist_step(centers, samples)
      i += 1
    }
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1-t0)/10e9 + "s.")
  }

  def for_run(samples:org.apache.spark.rdd.RDD[(Long, Array[Double])], max_iter:Int) : Double = {
    var i:Int = 0
    val t0 = System.nanoTime()
    centers = initialize(samples)
    while(i < max_iter) {
      centers = for_step(centers, samples)
      i += 1
    }
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1-t0)/10e9 + "s.")
  }
}
