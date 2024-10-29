package org.apache.spark.partitioner

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.control.Breaks.{break, breakable}

class ReducePartitioner[K: ClassTag, V: Ordering](partitions: Int, val samplingRate: Double = 0.2) extends Partitioner {
  require(samplingRate > 0.0 && samplingRate <= 1.0, "Sampling rate must be between 0 and 1")
  // 采样数据的容器
  private var sampledData: Array[(K, V)] = _
  // 记录每个分区的剩余容量
  private var partitionResidualCapacities: Array[Int] = _
  // RDD的总大小
  private var rddSize: Int = _
  // 总样本大小
  private var sampleSize: Int = _
  // 单个分区的样本大小
  private var sampleSizePerPartition: Int = _
  // 记录第i个key被分配给第j个partition
  private var keyToPartitionMap: Map[K, Int] = Map()

  override def numPartitions: Int = partitions

  // 采样算法
  private def sample(dataRDD: RDD[(K, V)]): Array[(K, V)] = {
    rddSize = dataRDD.count().toInt
    sampleSize = (rddSize * samplingRate).toInt
    sampleSizePerPartition = math.ceil(sampleSize.toDouble / partitions).toInt
    val result: Array[(K, V)] = dataRDD.sample(withReplacement = false, samplingRate).mapPartitions(_.take(sampleSizePerPartition), preservesPartitioning = true).collect()
    result
  }

  // 初始化分区器
  def initialize(dataRDD: RDD[(K, V)]): Unit = {
    sampledData = sample(dataRDD)
    // 根据算法3，我们需要初始化分区剩余容量和采样数据的排序逻辑
    partitionResidualCapacities = Array.fill[Int](partitions)((rddSize - sampleSize) / partitions)
    sortAndAssignPartitions()
  }

  private def sortAndAssignPartitions(): Unit = {
    // 显式提供Ordering[V]的逆序
    // implicit val ord: Ordering[V] = implicitly[Ordering[V]].reverse
    // 1.根据Cm降序排列
    val sortedData: Array[(K, V)] = sampledData.sortBy(_._2)(implicitly[Ordering[V]].reverse)
    // 2.计算p_avg
    val totalCapacity = partitionResidualCapacities.sum
    val pavg = math.ceil(totalCapacity.toDouble / partitions).toInt
    // 3.为每个分区设置最大分配数据量
    partitionResidualCapacities = Array.fill(partitions)(pavg)
    // 4.为每个分区分配数据，直到达到平均容量或没有更多数据
    for ((k,v) <- sortedData) {
      var remainingValue: Int = v.asInstanceOf[Int] // 剩余需要分配的值
      // 5.尝试将键分配到分区，直到其值被完全分配
      breakable {
        while (remainingValue > 0) {
          // 6. 查找剩余容量最大的分区
          var (maxResidual, maxPartition) = partitionResidualCapacities.zipWithIndex.maxBy(_._1)
          if (maxResidual >= remainingValue) {
            // 7. 如果分区有足够的剩余容量，直接分配
            partitionResidualCapacities(maxPartition) -= remainingValue
            keyToPartitionMap = keyToPartitionMap + (k -> maxPartition) // 记录分配位置
            remainingValue = 0  // 分配完成，重置remainingValue
            break()
          }
          else if (maxResidual > 0) {
            // // 8. 如果分区剩余容量小于 remainingValue 但大于 0，分配尽可能多的值
            remainingValue -= maxResidual
            partitionResidualCapacities(maxPartition) = 0
            keyToPartitionMap = keyToPartitionMap + (k -> maxPartition) // 记录分配位置
          }
          else {
            break()
          }
        }
      }
      // 10. 如果在循环结束后仍有剩余值，使用默认的哈希分区算法分配
      if (remainingValue > 0) {
        val partitionIndex = Utils.nonNegativeMod(k.hashCode, numPartitions)
        keyToPartitionMap = keyToPartitionMap + (k -> partitionIndex)
      }
    }
  }

  override def getPartition(key: Any): Int = {
    key match {
      case k: K =>
        // 12. 从 keyToPartitionMap 获取分区索引，如果不存在则使用默认哈希算法
        keyToPartitionMap.getOrElse(k, Utils.nonNegativeMod(k.hashCode, numPartitions))
      case _ =>
        // 如果key不是K类型，抛出异常
        throw new IllegalArgumentException("Key type is not supported")
    }
  }
}

