object validParentheses {
  /*val brackets:Map[Char, Char]=Map[Char, Char](
    ')' -> '(',
    '}' -> '{',
    ']' -> '[',
  )*/

  def isValid(str:String): Boolean ={
    val strConverted=str.toList
    var parHolder:List[Char]=Nil
    var braHolder:List[Char]=Nil
    var rectHolder:List[Char]=Nil
    var res=true

    strConverted.foreach {
      case x@'(' => parHolder = parHolder :+ x
      case x@'{' => braHolder = braHolder :+ x
      case x@'[' => rectHolder = rectHolder :+ x
      case x@')' if parHolder.nonEmpty => parHolder=parHolder.dropRight(1)
      case x@')' if parHolder.isEmpty => res=false
      case x@'}' if braHolder.nonEmpty => braHolder=braHolder.dropRight(1)
      case x@'}' if braHolder.isEmpty => res=false
      case x@']' if rectHolder.nonEmpty => rectHolder=rectHolder.dropRight(1)
      case x@']' if rectHolder.isEmpty => res=false

    }
    if (parHolder.isEmpty && braHolder.isEmpty && rectHolder.isEmpty){

    }else{
      res=false
    }
    res

  }

  def main(args: Array[String]): Unit = {
    println(isValid("(]"))
  }
}
