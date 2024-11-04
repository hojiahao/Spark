package org.apache.spark.partitioner

import org.apache.spark.partitioner.CustomizedPartitioner.uniqueHash
import org.apache.spark.util.Utils
import org.apache.spark.Partitioner

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3


class TimeShiftedPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(uniqueHash(key), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: TimeShiftedPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

private object TimeShiftedPartitioner {
  private def uniqueHash[K : ClassTag](key: K): Int = {
    // 获取当前时间戳（毫秒或纳秒级别）
    val timeStamp = System.nanoTime()
    // 将键和时间戳组合，确保唯一性
    val combinedHash = (key, timeStamp).hashCode()
    // 使用 MurmurHash3 生成哈希值以确保散列效果
    MurmurHash3.mixLast(combinedHash, timeStamp.hashCode)
  }
}



