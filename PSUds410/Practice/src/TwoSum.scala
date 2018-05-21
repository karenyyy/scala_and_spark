import scala.util.control.Breaks._

object TwoSum {

  def twoSum(nums:List[Int], target:Int):List[Int]={
    val tuple=nums.zipWithIndex
    var index= -1
    var startidx=0
    for ((num, i)<-tuple){
      val rest=target-num
      println(rest)
      if (nums.contains(rest)){
        startidx=nums.indexOf(num)
        index=nums.indexOf(rest)
      }
    }
    List(startidx, index)
  }

  def main(args: Array[String]): Unit = {
    val nums=List(2,7,11,15)
    println(twoSum(nums, 9))
  }

}
