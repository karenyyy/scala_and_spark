import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row

object basicStat {
  def main(args: Array[String]): Unit = {


    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val a=Array(Map("name"->"1", "name"->"1", "name"->"1"))
    println(a(0).get("name1"))

    val first::rest=List(1,2,3)
    println(first)

    val even=List.tabulate(10)(1 to 10)
    even.foldLeft(0){_+_}

    val aList = List(1, 2, 3)
    val bList = List(1, 2, 3)

    val lst=aList.zip(bList).map{case (x1, x2)=>x1+x2}.reduce(_+_)
    println(lst)



  }



}
