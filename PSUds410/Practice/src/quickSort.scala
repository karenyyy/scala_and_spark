object quickSort{

  def quickSort(list:Array[Int])={
    def swap(index1:Int,index2:Int)={
      val t=list(index1)
      list(index1)=list(index2)
      list(index2)=t
    }

    def quickSortHelper(low:Int, high:Int):Unit={
      var left=low
      var right=high
      val pivot= list((low+high)/2)
      val n=high-low
      if (n<=1){
        list
      }

      while (left<=right) {
        while (list(left) < pivot) {
          left += 1
        }
        while (list(right) > pivot) {
          right -= 1
        }
        if (left <= right) {
          swap(left, right)
          left += 1
          right -= 1
        }
      }

      if (low<right) {
        quickSortHelper(low, right)
      }
      if (right<high) {
        quickSortHelper(left, high)
      }
    }

    quickSortHelper(0,list.length-1)

  }


  def sort(xs: Array[Int]) {
    def swap(i: Int, j: Int) {
      val t = xs(i); xs(i) = xs(j); xs(j) = t
    }
    def sort1(l: Int, r: Int) {
      val pivot = xs((l + r) / 2)
      var i = l; var j = r
      while (i <= j) {
        while (xs(i) < pivot) i += 1
        while (xs(j) > pivot) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (l < j) sort1(l, j)
      if (j < r) sort1(i, r)
    }
    sort1(0, xs.length - 1)
  }


  def main(args: Array[String]): Unit = {
    val arr = Array(3,5,7,3,2,6,8,3,5,1,5,2)
    sort(arr)
  }

}