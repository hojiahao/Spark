package cn.edu.szu.dataskew.sort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.common.ConfigurableOrderedRDDFunctions
import org.apache.spark.partitioner.DensityAwarePartitioner
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.math.sqrt
import scala.reflect.ClassTag

object DensityAwarePartitionerSort {

  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]): ConfigurableOrderedRDDFunctions[K, V, (K, V)] = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]): Unit = {
    val map_parallelism = args(1).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("DensityAwarePartitionerSort")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    // 记录开始时间
    val jobStartTime = System.currentTimeMillis()
    val filepath = args(0)
    val lines: RDD[String] = sc.textFile(filepath, map_parallelism)
    val mapStartTime = System.currentTimeMillis()
    // 使用flatMap来分割单词，并按照空格、点和逗号分割
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    val mapEndTime = System.currentTimeMillis()
    println("Map Time: " + (mapEndTime - mapStartTime) + "ms")
    // 记录 reduce 阶段开始时间
    val reduceStartTime = System.currentTimeMillis()
    // 使用默认分区器
    val partitioner: DensityAwarePartitioner[String, Int] = new DensityAwarePartitioner[String, Int](parallel, wordToOne)
    val sorted = wordToOne.sortByKeyWithPartitioner(partitioner = partitioner)
    // 记录 reduce 阶段结束时间
    val reduceEndTime = System.currentTimeMillis()
    // 记录结束时间
    val jobEndTime = System.currentTimeMillis()
    println("Reduce Time: " + (reduceEndTime - reduceStartTime) + "ms")
    println(s"Total Job Execute Time: ${jobEndTime - jobStartTime}" + "ms")

    val partitionLengths: Array[(Int, Int)] = sorted.mapPartitionsWithIndex {
      (index, data) => {
        var len = 0
        while (data.hasNext) {
          len += 1 // 计数分区中的元素数量
          data.next()
        }
        Iterator((index, len))
      }
    }.collect()

    // Compute partition sizes for COV calculation
    val partitionSizes = partitionLengths.map({ case (index, len) => len })
    // Compute average size of partitions
    val mean = partitionSizes.sum.toDouble / partitionSizes.length
    // Compute standard deviation
    val variance = partitionSizes.map(size => math.pow(size - mean, 2)).sum / partitionSizes.length
    val stddev = sqrt(variance)
    // Compute COV (Coefficient of Variation)
    val cov = stddev / mean
    println("cov:" + cov)

    // 关闭spark连接
    sc.stop()
  }
}
