
/**
  * Created by karen on 17-9-19.
  */
object addNums {
  def adjustResult(result:IndexedSeq[Int])=result
    .map(_%10)
    .tail
    .reverse
    .filter(_!=0)
    .mkString

  def addNums(x:String, y:String)={
    val pad_size=(x.length max y.length)+1
    val padded_x="0"*(pad_size-x.length)+x
    val padded_y="0"*(pad_size-y.length)+y
    adjustResult((padded_x zip padded_y).scanRight(0){
      case ((dx, dy), last)=>dx.asDigit+dy.asDigit+last/10
    })
  }

  def multByDigit(num:String, digit:Int)=adjustResult(("0"+num).scanRight(0)(_.asDigit*digit+_/10))

  def main(x:String, y:String)={
    y.foldLeft("")((acc, digit)=>addNums(acc+"0", multByDigit(x, digit)))
  }
}
