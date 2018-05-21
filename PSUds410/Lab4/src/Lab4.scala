
case class Node(var leftChild: Node, var rightChild: Node, value: Int)

object Lab4 {

  def wordCount(filename:String)={

    val pattern="(http(s)?:/(/[A-Za-z0-9.\\-_]+)*|[A-Za-z]+(\')?[A-Za-z]+)".r

    val textFile = scala.io.Source.fromFile(filename)

    val wordcount = textFile
      .getLines()
      //.flatMap(_.split("\\s+"))
      .flatMap(x=>pattern.findAllIn(x)).foreach(println(_))
      //.size
      //.length

    /*

    val wordcount= {
      val counts = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
      textFile
        .getLines
        .flatMap(x => pattern.findAllIn(x).toList)
        .foreach(word => counts(word) += 1)

      counts.values.sum
    }



    val wordcount =textFile
      .getLines
      .flatMap(x=>pattern.findAllIn(x).toList)
      .foldLeft(Map.empty[String, Int]){
        (word_count, word) => word_count + (word -> (word_count.getOrElse(word, 0) + 1))
      }
      .values.sum

     */

    wordcount

  }

  @annotation.tailrec
  def gcd_helper(a: Int,b: Int): Int = {

    if(b == 0) a else gcd_helper(b, a%b)
  }


  def gcd(x: Int, y: Int) = {

    gcd_helper(x,y)


  }



  def cube(n: Int):Int={
    val list= 1 to n
    val result= list
      .map(x=>x*x*x)
      .map(_.toString)
      .filter(x=>x.charAt(0).toInt%2==0)
      //.filter(x=>x.slice(0,1).toInt%2==0)
      .map(_.toInt)
      .sum

    result

  }




  def nodesum(root: Node):Int = {
    def nodesum_helper(root: Node, result: Int): Int = root match {
        case null => result
        case Node(leftChild, rightChild, x)  => x+nodesum_helper(leftChild, result)+nodesum_helper(rightChild, result)

    }
    nodesum_helper(root,0)
  }



 /*
  def nodesum(root: Node):Int = {
    root match {
      case Node(null, null, x)  => x
      case Node(null, rightChild, x)  => x+nodesum(rightChild)
      case Node(leftChild, null, x)  => x+nodesum(leftChild)
      case Node(leftChild, rightChild, x)  => x+nodesum(leftChild)+nodesum(rightChild)

    }
  }

  */

}



