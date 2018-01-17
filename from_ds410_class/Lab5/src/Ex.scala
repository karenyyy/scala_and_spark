import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Ex {
  val conf=new SparkConf().setMaster("local").setAppName("Ex")
  val sc=new SparkContext(conf)

  def run(sc:SparkContext)={
    println("hello")
  }

  def loadData(sc:SparkContext): RDD[String] ={
    sc.textFile("0.edges")
  }

  def prob1(sc:SparkContext): Array[String] ={
    val data=loadData(sc)
    data.take(5)
  }

  def prob2(sc:SparkContext): Unit ={
    val data=loadData(sc)
    val lineSize=data.map(x=>x.split("\t").size)
    val longLines=lineSize.filter(_>2)
    val numlongLines=longLines.count()
    longLines.foreach(println(_))
  }

  def prob3(sc:SparkContext)={
    val data=loadData(sc)
    val edges=data.map(x=> {
      val arr = x.split("\t")
      (arr(0).trim().toInt, arr(1).trim().toInt)  // src_node, dst_node --> edge_list
    }
    )
    edges.persist() // persisting=caching
    edges.take(5).foreach(println(_))

    val rev_edges=edges.map(x=>(x._2, x._1))
    rev_edges.take(5).foreach(println(_))

    rev_edges.persist() // reuse for operations later, similar to cache;


    val AnotB=edges.subtract(rev_edges).count()
    //edges.subtract(rev_edges).collect().foreach(println(_))

    val BnotA=rev_edges.subtract(edges).count()
    //rev_edges.subtract(edges).collect().foreach(println(_))
    if (AnotB==0 && BnotA==0) println("redundancy") else println("no redundancy") //true->redundancy

  }

  def prob4(sc:SparkContext)={
    val data=loadData(sc)
    val edges=data.map(x=> {
      val arr = x.split("\t")
      (arr(0).trim().toInt, arr(1).trim().toInt)  // src_node, dst_node --> edge_list
    }
    )
    edges.persist() // persisting=caching
    //edges.take(5).foreach(println(_))
    val sortedEdges=edges
      //.sortByKey()
      .filter(x=>x._1<x._2)

    // TEST WHETHER IT IS STILL REDUNDANT
    val rev_edges=sortedEdges.map(x=>(x._2, x._1))
    //rev_edges.take(5).foreach(println(_))

    rev_edges.persist() // reuse for operations later, similar to cache;


    val AnotB=sortedEdges.subtract(rev_edges).count()
    val BnotA=rev_edges.subtract(sortedEdges).count()
    if (AnotB==0 && BnotA==0) println("redundancy") else println("no redundancy") //true->redundancy
  }

  def prob5(sc:SparkContext)={
    val data=loadData(sc)
    val edges=data.map(x=> {
      val arr = x.split("\t")
      (arr(0).trim().toInt, arr(1).trim().toInt)  // src_node, dst_node --> edge_list
    }
    )
    edges.persist() // persisting=caching
    //edges.take(5).foreach(println(_))

    val adjList=edges.groupByKey().sortByKey()

    adjList.foreach(println(_))

  }

  def toyGraph(sc:SparkContext)={
    val localGraph=List((1,2), (2,1), (2,3), (1,3), (3,1), (3,2), (1,4), (3,5), (4,1), (5,3))
    val rddGraph=sc.parallelize(localGraph)
    //val groupedGraph=rddGraph.groupByKey()
    rddGraph

  }


  def triangle(data:RDD[(Int, Int)])={
    val joined=data.join(data).filter(x=>x._2._1<x._2._2).map(x=>(x._2, x._1))
    val edges=data.map(x=>(x,None))
    val tri=joined.join(edges)
    tri.persist()
    //val tri_number=tri.count()/3
    //tri_number
    tri
  }

  def pageRank(sc:SparkContext)={
    // yahoo 1, google 2, psu 3, mycompany 4
    val adj_list=List((1,List(2,3)), (2,List(1,4)), (3,List(2)))
    val rdd_adj=sc.parallelize(adj_list)
    var rank=rdd_adj.mapValues(_ =>0.85)

    var iteration=0
    val max_iteration=10

    while (iteration<max_iteration) {
      val rank_list = rdd_adj.join(rank).values
      val updated_rank_list = rank_list
        .flatMap { case (adj_list, r) => adj_list.map(x => (x, r / adj_list.size)) }
      rank = updated_rank_list.reduceByKey(_+_).mapValues(x => x + 0.15)
      iteration+=1
    }
    rank.collect().foreach(println(_))

  }

  def TriCounter(sc:SparkContext)={
    val data=toyGraph(sc)
    val init_counter=data.join(data).mapValues(x=>0).reduceByKey(_+_)
    val after_counter=triangle(data).mapValues(x=>(x._1, 1)).map(x=>x._2)
    val result=init_counter.cogroup(after_counter)
      .mapValues{ case (x,y)=>(x.toList.sum+y.toList.sum)}
      .sortByKey()
    result

  }

  def bfsGraph(sc:SparkContext)={
    val localGraph=List(
      ("n1", List(("n2", 10), ("n3", 5))),
      ("n2", List(("n4", 1), ("n3", 3))),
      ("n3", List(("n2", 2), ("n4", 9), ("n5", 9))),
      ("n4", List(("n5", 4))),
      ("n5", List(("n1", 7), ("n4", 6)))

    )
    sc.parallelize(localGraph)
  }


  def bfs(sc: SparkContext, startNode:String="n1", num_nodes_to_use:Int=2) = {
    def addInf(x:Int, y:Int)={
      if (x==Int.MaxValue||y==Int.MaxValue) Int.MaxValue else x+y
    }
    val graph = bfsGraph(sc).partitionBy(new org.apache.spark.HashPartitioner(num_nodes_to_use)).persist()
    //initialize estimate for shortest path and back pointer
    val initBack:Option[String]=None
    val est = graph.map{x => (x._1, (if(x._1 == startNode) 0 else Int.MaxValue , initBack))}
    // ("n1", (0, none)), ("n2", (inf, none)), .....
    val finalresult=(1 until 10).foldLeft(est) { (accum, next) =>
      val joined = graph.join(accum)
      // ("n1", (List(...adj...), (0, backpointer))), ("n2", (List(...adj...), (inf, backpointer))), ...
      val candidates = joined.flatMap { x =>
        val mynode = x._1
        val mydist = x._2._2._1
        val mybackpointer = x._2._2._2
        val adjList = x._2._1
        val neighborMessages = adjList.map(x => (x._1, (addInf(x._2, mydist), Some(mynode))))
        val existing = (mynode, (mydist, mybackpointer))
        existing :: neighborMessages
      }
      candidates.reduceByKey((accum, next) =>
        if (next._1 < accum._1) next else accum // get the node with smaller distance to be the next dst
      )

    }
    finalresult
  }


  def main(args: Array[String]): Unit = {
    //pageRank(sc)
    triangle(toyGraph(sc)).foreach(println(_))
    //TriCounter(sc).foreach(println(_))
    //bfs(sc).foreach(println(_))
    prob3(sc)

  }

}
