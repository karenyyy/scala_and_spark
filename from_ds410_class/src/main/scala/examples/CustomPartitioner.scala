
import org.apache.spark.Partitioner

class CustomPartitioner(partition: Int) extends Partitioner{
  require(partition>0, s"partition number can not be negative!")
  
  def numPartitions: Int=partition
  
  def getPartition(key: Any):Int=key match {
    case (_:String, _: Int) => math.abs(key.hashCode()%numPartitions)
    case null => 0
    case _ => math.abs(key.hashCode()%numPartitions) // if not provided, use the key provided in getPsr function in the first place;
  }
  
  override def equals(other: Any):Boolean=other match {
    case x:CustomPartitioner => x.numPartitions==numPartitions
    case _ => false
  }
  
  override def hashCode: Int = numPartitions
}