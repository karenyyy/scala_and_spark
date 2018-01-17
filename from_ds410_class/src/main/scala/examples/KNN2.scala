
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD


object KNN2 {
  
    val conf = new SparkConf(false).setMaster("local").setAppName("KNN2")
    val sc = new SparkContext(conf)

    val k:Int = 6
    val path = "hdfs://master:9000/knn.txt"
    val data = sc.textFile(path).map(line =>{
      val pair = line.split("\\s+")
      (pair(0).toDouble,pair(1).toDouble ,pair(2))
    })
    val total:Array[ RDD[(Double,Double,String)] ] = data.randomSplit(Array(0.7,0.3))
    val train = total(0).cache()
    val test = total(1).cache()
    train.count()
    test.count()
    val bcTrainSet = sc.broadcast(train.collect())

    val bck = sc.broadcast(k)

    val resultSet = test.map{ line => {
      val x = line._1
      val y = line._2
      val trainDatas = bcTrainSet.value
      val set = scala.collection.mutable.ArrayBuffer.empty[(Double, String)]
      trainDatas.foreach(e => {
        val tx = e._1.toDouble
        val ty = e._2.toDouble
        val distance = Math.sqrt(Math.pow(x - tx, 2) + Math.pow(y - ty, 2))
        set.+= ((distance, e._3))
      })
      val list = set.sortBy(_._1)
      val categoryCountMap = scala.collection.mutable.Map.empty[String, Int]
      val k = bck.value
      for (i <- 0 until k){
        val category = list(i)._2
        val count = categoryCountMap.getOrElse(category, 0) + 1
        categoryCountMap += (category -> count)
      }
      val (rCategory,frequency) = categoryCountMap.maxBy(_._2)
      (x, y, rCategory)
    }}

    resultSet.repartition(1).saveAsTextFile("hdfs://master:9000/knn/result")
  
}