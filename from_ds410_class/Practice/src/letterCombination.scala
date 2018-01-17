object letterCombination {

  // the number of characters mapping of 1-9 on the keyboard
  val counts = Vector(3, 3, 3, 3, 3, 4, 3, 4)

  def decode(c: Char): Seq[Char] = {
    val index = c.toInt - '2'.toInt
    for (i <- 0 until counts(index)) yield (counts.take(index).sum + 'a'.toInt + i).toChar
  }

  val mappings: Map[Char, Seq[Char]] = (for (i <- '2' to '9') yield i -> decode(i)).toMap

  def letterCombinations(digits: String): List[String] = {
    var holder: List[String] = Nil

    if (digits.isEmpty){
      holder
    }else {
      val letters: Array[Char] = Array.fill(digits.length){' '}

      def generate(index: Int): Unit = {
        for (c <- mappings(digits(index))) {
          letters(index) = c
          if (index == 0) {
            holder = letters.mkString("") :: holder
            //println(holder, "holder")
          } else {
            generate(index - 1)
          }
        }
      }

      generate(digits.length - 1)
      //println(holder, "final")
      holder
    }
  }


  def main(args: Array[String]): Unit = {
    letterCombinations("2453832")
  }

}
