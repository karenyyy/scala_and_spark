
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by karen on 17-9-19.
  */
object AvgMap {
  case class AvgCount(var total:Int =0, var count:Int =0){
    def merge(other:AvgCount):AvgCount={
      total+=other.total
      count+=other.count
      this
    }
    def merge(input: Iterator[Int]):AvgCount={
      input.foreach{
        x=>
        total+=x
        count+=1
      }
      this
    }

    def avg():Float={
      total/count.toFloat
    }
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName("AvgCount")
    val sc=new SparkContext(conf)
    val input=sc.parallelize(List(1,2,3,4))
    val res=input
      .mapPartitions(x=>Iterator(AvgCount(0,0).merge(x)))
      .reduce((x,y)=>x.merge(y))
      .avg()

    println(res)

  }
}
