object shortestWordDistance {

  def shortestDistance(words:List[String], word1:String, word2:String):Int={
    var minDist=Int.MaxValue

    var (start, end)=(-1, -1)
    for ((w, index)<- words.zipWithIndex){
      if (w==word1){start=index}
      else if (w==word2){end=index}
      if (start!= -1 && end != -1){minDist=minDist min Math.abs(end-start)}
    }
    minDist
  }


  def main(args: Array[String]): Unit = {
    val words = List("practice", "makes", "perfect", "coding", "makes")
    println(shortestDistance(words, "coding", "practice"))
    println(shortestDistance(words, "makes", "coding"))
  }
}
