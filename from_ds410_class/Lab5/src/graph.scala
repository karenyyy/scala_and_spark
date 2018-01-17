import org.apache.spark.sql.SparkSession

/**
  * Created by karen on 10/19/17.
  */


object graph {

  type Vertex=Int
  type Graph=Map[Vertex, List[Vertex]]
  // represent graph as the adjacent list
  val g:Graph=Map(1->List(2,4), 2->List(1,3), 3->List(2,4), 4->List(1,3))

  //  1---2
  //  |   |
  //  4---3

  def DFS(start: Vertex, g: Graph): List[Vertex] = {

    def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
      if (visited.contains(v))
        visited
      else {
        val neighbours: List[Vertex] = g(v) filterNot visited.contains

        neighbours.foldLeft(visited++List(v))((b, a) => DFS0(a, b))
      }
    }

    DFS0(start, List())
  }


  def BFS(start: Vertex, g: Graph): List[Vertex] = {

    def BFS0(elems: List[Vertex],visited: List[Vertex]): List[Vertex] = {
      val newNeighbors = elems.flatMap(g(_)).filterNot(visited.contains).distinct
      if (newNeighbors.isEmpty)
        visited
      else
        BFS0(newNeighbors, visited++newNeighbors)
    }

    BFS0(List(start),List(start))
  }

  def main(args: Array[String]): Unit = {
    println(DFS(1, g))
    println(BFS(1, g))
  }

}