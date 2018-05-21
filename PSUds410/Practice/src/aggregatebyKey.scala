import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object aggregatebyKey {
  val conf=new SparkConf().setMaster("local").setAppName("")
  val sc=new SparkContext(conf)


  def addToLeft(accum: Array[Double], value: Vector[Double]):Array[Double] = {
    value.zipWithIndex.foreach(z => accum(z._2) += z._1)
    accum
  }

  def aggByKey(args: Array[String]): Unit = {
    val keysWithValuesList = if (args==null) Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D"); else args

    val data = sc.parallelize(keysWithValuesList)

    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).persist()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v


    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
  }

  def avg()={
    val input = List.range(1,18301)
    val pair=input.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    println(pair._1/pair._2)
  }


  def avg2():Double={
    val input=1 to 18
    val init=input.map(x=>(x,1))
    val res =input.foldLeft(0)((x, y)=> x+y)
    res/input.length
  }

  def main(args: Array[String]): Unit = {
    println(avg2())
    addToLeft(Array(0.1,0.2,0.3,0.4,0.5), Vector(0.51,0.42,0.33,0.24,0.15)).foreach(println(_))
  }
}
