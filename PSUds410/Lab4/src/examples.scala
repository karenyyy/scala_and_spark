/**
  * Created by karen on 17-10-5.
  */
object examples {
  def quickSort(arr:Array[Int]):Array[Int]={
    if (arr.length<=1) {
      arr
    }
    else {
      val pivot = arr(arr.length / 2)
      // <pivot + ==pivot + >pivot
      Array.concat(quickSort(arr filter (pivot >)), quickSort(arr filter (pivot ==)), quickSort(arr filter (pivot <)))
    }
  }


  def sqrtByNewtonMethod(estimate:Double, x:Double):Double={
    if (scala.math.abs((estimate*estimate)-x)<0.001) estimate
    else sqrtByNewtonMethod((estimate+x/estimate)/2, x)
  }

  def sum(f:Int=>Int, x:Int, y:Int):Int={
    if (x==y) x
    else f(x) + sum(f, x + 1, y)
  }

  def sumInts(a: Int, b: Int): Int = sum((x: Int) => x, a, b)

  def sumSquares(a:Int, b:Int):Int = sum((x:Int)=>x*x, a:Int, b:Int)

  def sumCurrying(f:Int=>Int):(Int, Int)=>Int={
    def sumHelper(x:Int, y:Int):Int={
      if (x==y) x
      else f(x) + sum(f, x + 1, y)
    }
    sumHelper
  }

  def sumIntsC= sumCurrying((x: Int) => x)

  def sumSquaresC=sumCurrying((x:Int)=>x*x)

  def sumCurried(f:Int=>Int)(x:Int, y:Int):Int={
    def sumHelper:Int={
      if (x==y) x
      else f(x) + sum(f, x + 1, y)
    }
    sumHelper
  }


  def sumIntsCu= sumCurried((x: Int) => x)

  def sumSquaresCu=sumCurried((x:Int)=>x*x)


  def main(args: Array[String]): Unit = {
    val arr=Array(112091,121,3,44,58,6)
    quickSort(arr).foreach(println(_))

    println(sqrtByNewtonMethod(1.22222222, 2))
    println(sumInts(1, 100), sumIntsC(1, 100), sumCurried((x: Int) => x)(1, 100))
    println(sumSquares(1, 100), sumSquaresC(1, 100), sumCurried((x: Int) => x*x)(1, 100))

  }




}
