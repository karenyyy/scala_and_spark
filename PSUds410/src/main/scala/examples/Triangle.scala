import java.io.PrintWriter
import java.io.File
import java.util.Arrays

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try


object Triangle {
  private final val SPARK_MASTER="client";
  private final val APP_NAME="driver";
  
  // set hdfs configuration path;
  private final val CORES_SITE_CONFIG_PATH=new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
  private final val HDFS_SITE_CONFIG_PATH=new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);
    val sc=new SparkContext(conf);
    
    // configure hdfs
    val configuration=new Configuration();
    configuration.addResource(CORES_SITE_CONFIG_PATH);
    configuration.addResource(HDFS_SITE_CONFIG_PATH);
    
    // parsing lines;
    val lines=sc.textFile("hdfs:/ds410/lab4/CSNNetwork.csv")
    val item=lines.map ( line => line.split(",")  );
    val int_item=item.filter ( i => Try(i(1).toInt).isSuccess&& Try(i(2).toInt).isSuccess );
    
    // convert to vertex pair;
    val vertex_pairs=int_item.map ( item => (item(1).toInt, item(2).toInt)).filter(item=>item._1!=item._2);
    
    //permute edge
    val edge_increase=vertex_pairs.map(vertex_pair=>(if (vertex_pair._1<vertex_pair._2) (vertex_pair._1, vertex_pair._2) else (vertex_pair._2, vertex_pair._1))).distinct();
    val edge_decrease=vertex_pairs.map(vertex_pair=>(if (vertex_pair._1>vertex_pair._2) (vertex_pair._1, vertex_pair._2) else (vertex_pair._2, vertex_pair._1))).distinct();
    
    val two_edge=edge_increase.join(edge_decrease).map(x=>(x._2, x._1));
    
    val extended_edge_decrease=edge_decrease.map(x=>(x,1));
    val triangle=extended_edge_decrease.join(two_edge);
    val triTuple=triangle.map(x=>(x._1._1, x._1._2, x._2._2));
    
    val triCount=triTuple.flatMap(x=>List(x._1, x._2, x._3)).groupBy (identity).mapValues(_.size);
    
    // compute neighbor counts per node;
    val increase_group=edge_increase.groupByKey.mapValues(_.toList);
    val decrease_group=edge_decrease.groupByKey.mapValues(_.toList);
    
    // x=>x 就是identity函数的一个适用的例子
    
    /*scala> val a: Array[Int] = Array(1, 12, 3, 4, 1)
			a: Array[Int] = Array(1, 12, 3, 4, 1)

			scala> a.groupBy(i=>i)
			res1: scala.collection.immutable.Map[Int,Array[Int]] = Map(4 -> Array(4), 1 -> Array(1, 1), 3 -> Array(3), 12 -> Array(12))
			
			scala> a.groupBy(identity)
			res3: scala.collection.immutable.Map[Int,Array[Int]] = Map(4 -> Array(4), 1 -> Array(1, 1), 3 -> Array(3), 12 -> Array(12))

			scala> a.groupBy(identity).mapValues { _.length }
			res4: scala.collection.immutable.Map[Int,Int] = Map(4 -> 1, 1 -> 2, 3 -> 1, 12 -> 1)
			*/
    
    val nodeCount=increase_group.union(decrease_group).groupByKey.mapValues(_.flatMap(x=>x).toList.length)
    
    // at last, compute network cluster coefficient:
    val res=triCount.fullOuterJoin(nodeCount).mapValues(x=>if (x._1!=None) (x._1.get, (x._1.get/(x._2.get*(x._2.get-1)/2).toDouble)) else (0,0)).sortByKey().collect();
    
    val writer=new PrintWriter(new File(".....csv"));
    res.foreach(x=>writer.write(x._1+"\t"+x._2._1+"\t"+x._2._2+"\n"));
  }
  
}