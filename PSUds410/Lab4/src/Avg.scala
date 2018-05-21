import org.apache.spark.{SparkConf, SparkContext}

object Avg {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local").setAppName("Avg")

    val sc = new SparkContext(conf)
    val input = sc.parallelize(List.range(1,18301))

    val result = input
      .aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avg = result._1 / result._2.toFloat
    println(avg)

  }

}