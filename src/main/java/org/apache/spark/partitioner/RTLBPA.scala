package org.apache.spark.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

class RTLBPA[K: ClassTag, V](partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  val hashContianer = new mutable.HashSet[Int]()
  val keyContainer = new mutable.HashSet[K]()
  val keyHashContainer = new mutable.HashMap[K, Int]()
  val random = new Random(1)

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val keyHash = k.hashCode
    var partitionNum = 0
    if (!keyContainer.contains(k) && !hashContianer.contains(keyHash)) {
      keyContainer.add(k)
      hashContianer.add(keyHash)
      keyHashContainer.put(k, keyHash)
      partitionNum = Utils.nonNegativeMod(keyHash, numPartitions)
    }
    else if (!keyContainer.contains(k) && hashContianer.contains(keyHash)) {
      keyContainer.add(k)
      val rand = random.nextInt(numPartitions)
      val newHashValue = keyHash + rand
      keyHashContainer.put(k, newHashValue)
      partitionNum = Utils.nonNegativeMod(newHashValue, numPartitions)
    }
    else {
      val realHash = keyHashContainer.apply(k)
      partitionNum = Utils.nonNegativeMod(realHash, numPartitions)
    }
    partitionNum
  }
}

