import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Try


object triangle {

  val conf=new SparkConf().setMaster("local").setAppName("")
  val sc=new SparkContext(conf)

  def toyGraph(sc: SparkContext) = {
    val localGraph = List(
      (1, 2),
      (2, 1),
      (2, 3),
      (1, 3),
      (3, 1),
      (3, 2),
      (1, 4),
      (3, 5),
      (4, 1),
      (5, 3)
    )
    val rddGraph = sc.parallelize(localGraph) // sent this to paraller workers
    rddGraph
  }


  def triAlgorithm()= {
    val lines = toyGraph(sc)
    val vertexPairValid = lines.filter(x => Try(x._1.toInt).isSuccess && Try(x._2.toInt).isSuccess)
    val edgeTuple = vertexPairValid.map(x => (x._1.toInt, x._2.toInt)).filter(x => x._1 != x._2).persist()

    val edgeIncrease = edgeTuple.map(x => if (x._1 < x._2) (x._1, x._2); else (x._2, x._1)).distinct()
    val edgeDecrease = edgeTuple.map(x => if (x._1 > x._2) (x._1, x._2) else (x._2, x._1)).distinct()

    val twoEdge = edgeIncrease.join(edgeDecrease).map(x => (x._2, x._1)) // since edgeIncrease at first;
    // thus, if there exists tuple2 in x._2, then it must be in decreasing order
    // thus, later should use edgeDecrease to join, to get triangle

    val tricount = 1
    val extended = edgeDecrease.map(x => (x, tricount))
    val triPair = extended.join(twoEdge).map(x => (x._1._1, x._1._2, x._2._2))
    //val triCount = extended.join(twoEdge).map(x => ((x._1._1, x._1._2), x._2._1)).reduceByKey(_ + _)
    val triCount = triPair.flatMap(x=>List(x._1,x._2, x._3)).groupBy(identity).mapValues(_.size)
    // cal each edge of a formed triangle

    val increase_neighbour = edgeIncrease.groupByKey().mapValues(_.toList)
    val decrease_neighbour = edgeDecrease.groupByKey().mapValues(_.toList)

    val nodeCount=increase_neighbour.union(decrease_neighbour).groupByKey.mapValues(_.flatten.toList.length)

    triCount.foreach(println(_))
    nodeCount.foreach(println(_))
    triCount.fullOuterJoin(nodeCount).foreach(println(_))
    triCount.fullOuterJoin(nodeCount).mapValues(x=> if(x._1.isDefined) (x._1.get, x._1.get / (x._2.get*(x._2.get-1)/2)) else (0,0)).sortByKey(true).foreach(println(_))

    triCount.fullOuterJoin(nodeCount).mapValues(x=> if (x._1 isDefined) (x._1.get, x._2.get, x._1, x._2)).foreach(println(_)) // get: extract from some or Option

  }

  def main(args: Array[String]): Unit = {
    triAlgorithm()
  }


}
