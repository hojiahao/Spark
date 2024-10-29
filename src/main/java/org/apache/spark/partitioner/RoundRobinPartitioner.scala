package org.apache.spark.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

class RoundRobinPartitioner[K : ClassTag, V](partitions: Int, rdd: RDD[_ <: Product2[K, V]]) extends Partitioner {

  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  // 总的数据量
  private val dataSize = rdd.count()
  // 每个分区允许的最大数据量
  private val dataSizePerPartition = math.ceil(1.0 * dataSize / partitions).toInt
  // 每个分区的当前数据量
  private val currentPartitionCounts = Array.fill(partitions)(0L)
  // 存储键、哈希值、键到哈希值的映射关系
  private val keyContainer: mutable.Set[K] = mutable.HashSet[K]()
  private val hashContainer: mutable.Set[Int] = mutable.HashSet[Int]()
  private val keyToHashMap: mutable.HashMap[K, Int] = mutable.HashMap[K, Int]()

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case k: K =>
        var hashValue = k.hashCode()
        // 计算当前键的分区号
        var partition = Utils.nonNegativeMod(hashValue, numPartitions)
        // 判断当前分区是否已满
        while (currentPartitionCounts(partition) >= dataSizePerPartition) {
          // 生成新的哈希值，直到找到未满的分区
          hashValue += Random.nextInt(numPartitions)
          partition = Utils.nonNegativeMod(hashValue, numPartitions)
        }
        if (!keyContainer.contains(k) && !hashContainer.contains(hashValue)) {
          // 更新容器
          keyContainer.add(k)
          hashContainer.add(hashValue)
          keyToHashMap.put(k, hashValue)
          currentPartitionCounts(partition) += 1
          partition
        } else if (!keyContainer.contains(k) && hashContainer.contains(hashValue)) {
          // 键不在键容器中，但哈希值已经存在，需要生成新的哈希值
          hashValue += Random.nextInt(numPartitions)
          partition = Utils.nonNegativeMod(hashValue, numPartitions)
          // 再次判断分区是否已满
          while (currentPartitionCounts(partition) >= dataSizePerPartition) {
            hashValue += Random.nextInt(numPartitions)
            partition = Utils.nonNegativeMod(hashValue, numPartitions)
          }
          // 更新容器
          keyContainer.add(k)
          hashContainer.add(hashValue)
          keyToHashMap.put(k, hashValue)
          currentPartitionCounts(partition) += 1
          partition
        } else {
          // 键和哈希值都存在，直接返回对应的分区
          hashValue = keyToHashMap(k)
          partition = Utils.nonNegativeMod(hashValue, numPartitions)
          // 再次判断分区是否已满
          while (currentPartitionCounts(partition) >= dataSizePerPartition) {
            hashValue += Random.nextInt(numPartitions)
            partition = Utils.nonNegativeMod(hashValue, numPartitions)
          }
          partition
        }
      case _ => throw new IllegalArgumentException("Invalid key type")
    }
  }
}
