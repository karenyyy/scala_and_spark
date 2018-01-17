import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try

object KMeans {
  private final val SPARK_MASTER="client"
  private final val APP_NAME="driver"
  
  // set hdfs configuration path;
  private final val CORES_SITE_CONFIG_PATH=new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
  private final val HDFS_SITE_CONFIG_PATH=new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
  
  /*
  def ofDim[T]( n1: Int ): Array[T]
	创建数组给出的尺寸。
	def ofDim[T]( n1: Int, n2: Int ): Array[Array[T]]
	创建了一个2维数组
	def ofDim[T]( n1: Int, n2: Int, n3: Int ): Array[Array[Array[T]]]
	创建3维数组
	*/
  
  def initCluster(nb_cluster: Int, nb_feature: Int): Array[(Int, Array[Double])]={
    val clusters=Array.ofDim[(Int, Array[Double])](nb_cluster);
    for (i<- 0 to nb_cluster-1){
      clusters(i)=(i, Array.fill(nb_feature){scala.util.Random.nextDouble()});
    }
    return clusters;
  }
  
  
  def Distance(a:Array[Double], b:Array[Double]) : Double = {
        assert(a.length == b.length, "Distance(): features dim does not match.")
        var dist = 0.0
        for (i <- 0 to a.length-1) {
            dist = dist + math.pow(a(i) - b(i), 2)
        }
        return math.sqrt(dist)
    }
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);
    val sc=new SparkContext(conf);
    
    // configure hdfs
    val configuration=new Configuration();
    configuration.addResource(CORES_SITE_CONFIG_PATH);
    configuration.addResource(HDFS_SITE_CONFIG_PATH);
    
    // cluster numbers:
    val nb_cluster=3;
    val nb_feature=4;
    
    // readin and parse dataset;
    val lines=sc.textFile("hdfs:/ds410/lab5/iris.data");
    val samples=lines.map(x=>x.split(",").slice(0, 4).map(_.toDouble)).zipWithIndex().map(x=>(x._2,x._1));
    
    // broadcast centroid parameters to clusters;
    
    val clusters=sc.broadcast(Array((0,Array(5.1,3.5,1.4,0.2)), (1,Array(4.9,3.0,1.4,0.2)), (2,Array(4.7,3.2,1.3,0.2))));
    val dist=samples.flatMap(testdata=>clusters.value.map(clusterdata=>(testdata._1, (clusterdata._1, Distance(testdata._2, clusterdata._2)))));
    // dist=:
    /*
     * (number 1,(cluster 1, distance= ?))
     * (number 1,(cluster 2, distance= ?))
     * (number 1,(cluster 3, distance= ?))
     * ........
     * (number 2,(cluster 1, distance= ?))
     * (number 2,(cluster 2, distance= ?))
     * (number 2,(cluster 3, distance= ?))
     * ......
     * */
    
    val labels=dist.reduceByKey( (x,y)=> (if (x._2<y._2) x; else y)).map(x=>(x._1,x._2._1));
    // label is:
    /*
     * (number 1, cluster ?)
     * (number 2, cluster ?)
     * (number 3, cluster ?)
     * .....
     * */
    
    // recompute the clusters:
    var new_clusters=Array.ofDim[(Int, Array[Double])](nb_cluster);
    for (i<- 0 to nb_cluster-1){
      // now count samples in cluster i;
      val sample_in_cluster=samples.join(labels.filter(_._2==i));
      // sample_in_cluster:
      /*
       * (sample aa , (Array[(Int, Array[Double])], cluster1))
       * (sample bb , (Array[(Int, Array[Double])], cluster1))
       * (sample cc , (Array[(Int, Array[Double])], cluster1))
       * ......
       * */
      val total_number=sample_in_cluster.count;
      if (total_number!=0){
        // reduceByKey then get average;
        var tmp=sample_in_cluster.map(x=>x._2._1).reduce((x,y)=>x.zip(y).map{case(x,y) => x+y });
        // because can not directly add two arrays;
        // so must do x.zip(y).map{case(x,y)=>x+y};
        /* tmp is
         * sum(all Arrays of which cluster number is i)
         * */
        tmp=tmp.map(x=>x/total_number.toDouble);
        new_clusters(i)=(i, tmp);
        /*
         * tmp: the center of each feature;
         * */
      }
      else {
        // if no samples in this cluster, then randomly take one from samples;
        new_clusters(i)=(i, samples.takeSample(false, 1)(0)._2);
      }
    }
     val writer = new PrintWriter(new File("output.txt"));
		 new_clusters.foreach(x => x._2.foreach(y => writer.write(x._1 + "\t" + y + "\n")));
     writer.close();
  }
  
}