import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

object Ex {

    val conf=new SparkConf().setAppName("Optimization").setMaster("local")
    val sc=new SparkContext(conf)


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

    def toy_web_graph(sc: SparkContext, num_nodes_to_use:Int=2) = {
       //yahoo=1, google=2, psu=3, yourcompany=4
       val local_graph = List(
           (1, List(2, 3)),
           (2, List(1, 4)),
           (3, List(2)),
           (4, List[Int]()) //do we need this. It is redundant, or is it?
       ) 
       sc.parallelize(local_graph,num_nodes_to_use)
    }
 
    def pagerank(ingraph: org.apache.spark.rdd.RDD[(Int, List[Int])], num_nodes_to_use:Int=2) = {
         val graph = ingraph.partitionBy(new org.apache.spark.HashPartitioner(num_nodes_to_use)).persist()
         //initialization
         var ranks = graph.map{x => (x._1, 1.0)} 
         for (i <- 0 until 10) {
             val joined = graph.join(ranks)
             val messages = joined.flatMap{ x =>   //x._1 is node id, x._2._1 is adj graph, x._2._2 is rank
                 val myrank = x._2._2
                 val neighbors = x._2._1
                 val amount = myrank / neighbors.size
                 val send = neighbors.map(x => (x, amount))
                 send
             }
             val income = messages.reduceByKey((accum, next_value) => accum + next_value)
             ranks = income.mapValues(x => .15 + x * .85)
         }
         ranks
    }
    def bfsGraph(sc: SparkContext) = {
       val localGraph = List(
          ("n1", List(("n2", 10), ("n3", 5))),
          ("n2", List(("n4", 1), ("n3", 3))),
          ("n3", List(("n2", 2), ("n4", 9), ("n5", 2))),
          ("n4", List(("n5", 4))),
          ("n5", List(("n1", 7), ("n4", 6)))
       )
       sc.parallelize(localGraph,2)

    }

    def bfs(sc: SparkContext, startNode:String="n1", num_nodes_to_use:Int=2) = {
       //allows us to add ints to MaxInt and get MaxInt back (pretend MaxInt is infinity)
       //doesn't handle the case where a+b > MaxInt (overflow will occur)
       def addInf(a: Int, b: Int) = {
            if(a == Int.MaxValue || b == Int.MaxValue) Int.MaxValue
            else a+b
       }
       val graph = bfsGraph(sc).partitionBy(new org.apache.spark.HashPartitioner(num_nodes_to_use)).persist()
       //initialize estimate for shortest path and back pointer
       val initBack: Option[String] = None
       var est = graph.map{x => (x._1, (if(x._1 == startNode) 0 else Int.MaxValue , initBack))} // (k, (estimated distance to k, backpointer))
       for(i <- 1 until 10) {
           val joined = graph.join(est) // (k, (adj_list, (estimated distance to k, backpointer)))
           val candidates  = joined.flatMap{x =>
              val mynode = x._1
              val mydist = x._2._2._1 //current estimated distance for mynode
              val mybackpointer = x._2._2._2
              val adjList = x._2._1
              val neighborMessages = adjList.map(z => (z._1, (addInf(mydist, z._2), Some(mynode))))  //candidate estimates for neighbors
              val existing = (mynode, (mydist, mybackpointer)) //existing best estimate for mynode
              val messages = existing::neighborMessages
              messages
           }
            // order by distance, next would be who has the smallest distance
           est = candidates.reduceByKey{(accum, next) =>
              if(next._1 < accum._1) next else accum
           }
       }
       est
    }

    def betterBFS(sc: SparkContext, startNode:String="n1", num_nodes_to_use:Int=2) = {
       //allows us to add ints to MaxInt and get MaxInt back (pretend MaxInt is infinity)
       //doesn't handle the case where a+b > MaxInt (overflow will occur)
       def addInf(a: Int, b: Int) = {
            if(a == Int.MaxValue || b == Int.MaxValue) Int.MaxValue
            else a+b
       }
       val graph = bfsGraph(sc).partitionBy(new org.apache.spark.HashPartitioner(num_nodes_to_use)).persist()
       //initialize estimate for shortest path and back pointer
       val initBack: Option[String] = None
       val est = graph.map{x => (x._1, (if(x._1 == startNode) 0 else Int.MaxValue , initBack))}
       val finalResult = (1 until 10).foldLeft(est){ (accum, next) => //next is an integer from 1 to 10
           val joined = graph.join(accum) // (k, (adj_list, (estimated distance to k, backpointer)))
           val candidates  = joined.flatMap{x =>
              val mynode = x._1
              val mydist = x._2._2._1 //current estimated distance for mynode
              val mybackpointer = x._2._2._2
              val adjList = x._2._1
              val neighborMessages = adjList.map(z => (z._1, (addInf(mydist, z._2), Some(mynode))))  //candidate estimates for neighbors
              val existing = (mynode, (mydist, mybackpointer)) //existing best estimate for mynode
              val messages = existing::neighborMessages
              messages
           }
           //return next value of accumulator
           candidates.reduceByKey{(accum, next) =>
              if(next._1 < accum._1) next else accum
           }
       }
       finalResult
    }


    def trianglesWithPlan(sc: SparkContext) = {
       val data = sc.textFile("/ds410/facebook/").map(x => x.trim()).map(x => x.split("\t")).map(x => (x(0), x(1)))
       val joined = data.join(data).filter(x => (x._2._1 < x._2._2))
       joined.foreach(println(_))
       val rev_joined = joined.map{x => (x._2, x._1)}
       val tuple_key = data.map{x => (x , None)}
       val triangle_join = tuple_key.join(rev_joined)
       val num_triangles = triangle_join.count() / 3
       (num_triangles, triangle_join.toDebugString)
    }
    def trianglesWithPlan2(sc: SparkContext) = {
       val data = sc.textFile("/ds410/facebook/").map(x => x.trim()).map(x => x.split("\t")).map(x => (x(0), x(1))).persist()
       val joined = data.join(data).filter(x => (x._2._1 < x._2._2))
       val rev_joined = joined.map{x => (x._2, x._1)}
       val tuple_key = data.map{x => (x , None)}
       val triangle_join = tuple_key.join(rev_joined)
       val num_triangles = triangle_join.count() / 3
       (num_triangles, triangle_join.toDebugString)
    }
    def trianglesWithPlan3(sc: SparkContext) = {
       val data = sc.textFile("/ds410/facebook/").map(x => x.trim()).map(x => x.split("\t")).map(x => (x(0), x(1))).partitionBy(new HashPartitioner(10)).persist()
       val joined = data.join(data).filter(x => (x._2._1 < x._2._2))
       val rev_joined = joined.map{x => (x._2, x._1)}
       val tuple_key = data.map{x => (x , None)}
       val triangle_join = tuple_key.join(rev_joined)
       val num_triangles = triangle_join.count() / 3
       (num_triangles, triangle_join.toDebugString)
    }
    def trianglesWithPlan4(sc: SparkContext) = {
       val data = sc.textFile("/ds410/facebook/").map(x => x.trim()).map(x => x.split("\t")).map(x => (x(0), x(1)))
       val partdata = data.partitionBy(new HashPartitioner(10)).persist() 
       val joined = partdata.join(partdata).filter(x => (x._2._1 < x._2._2))
       val rev_joined = joined.map{x => (x._2, x._1)}
       val tuple_key = data.map{x => (x , None)}
       val triangle_join = tuple_key.join(rev_joined)
       val num_triangles = triangle_join.count() / 3
       (num_triangles, triangle_join.toDebugString)
    }

  def main(args: Array[String]): Unit = {
    prob3(sc)
  }
}
 


