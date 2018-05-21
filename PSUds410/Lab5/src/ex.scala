import org.apache.spark.SparkContext


object Exx {
  def run(sc: SparkContext) = {
    println("hello")
  }
  def loadData(sc: SparkContext) = {
    sc.textFile("/ds410/facebook/")
  }

  def prob1(sc: SparkContext) = { //return a few lines of dataset
    val data = loadData(sc)
    data.take(5)
  }

  //check that data really is edge list
  def prob2(sc: SparkContext) = {
    val data = loadData(sc)
    // count number of tab separated things in each line
    val lineSize  =  data.map(x => x.split("\t").size)
    val longLines   = lineSize.filter(x => x > 2)
    val numLongLines = longLines.count()
    if(numLongLines == 0) println("edge list") else println("adj")
    numLongLines
  }

  //determine if redundant or non-redundant edge list
  def prob3(sc: SparkContext) = {
    val data = loadData(sc)
    val edges  =  data.map(x => {val arr = x.split("\t"); (arr(0).trim().toInt, arr(1).trim().toInt)})
    edges.persist()
    val rev_edges = edges.map(x => (x._2, x._1))
    rev_edges.persist()
    val inAButNotInB = edges.subtract(rev_edges).count()
    val inBButNotInA = rev_edges.subtract(edges).count()
    if(inAButNotInB == 0 && inBButNotInA == 0) true else false
  }


  //create non-redundant  edge list
  def prob4(sc: SparkContext) = {
    val data = loadData(sc)
    val edges  =  data.map(x => {val arr = x.split("\t"); (arr(0).trim().toInt, arr(1).trim().toInt)})
    val nonredundant = edges.filter(x => (x._1 < x._2))
    nonredundant //this is rdd, won't be computed until action called on it
  }

  //create adjacency list from
  def prob5(sc: SparkContext) = {
    val data = loadData(sc)
    val edges  =  data.map(x => {val arr = x.split("\t"); (arr(0).trim().toInt, arr(1).trim().toInt)})
    val grouped = edges.groupByKey()
    grouped
  }

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
    val rddGraph = sc.parallelize(localGraph, 3)
    rddGraph
  }

  def triangle(data: org.apache.spark.rdd.RDD[(Int, Int)]) = {
    val joined = data.join(data).filter(x => (x._2._1 < x._2._2))
    val rev_joined = joined.map{x => (x._2, x._1)}
    val tuple_key = data.map{x => (x , None)}
    val triangle_join = tuple_key.join(rev_joined)
    val num_triangles = triangle_join.count() / 3
    num_triangles
  }

  def toy_web_graph(sc: SparkContext) = {
    //yahoo=1, google=2, psu=3, yourcompany=4
    val local_graph = List(
      (1, List(2, 3)),
      (2, List(1, 4)),
      (3, List(2)),
      (4, List[Int]()) //do we need this. It is redundant, or is it?
    )
  }
}

