package cn.edu.szu.dataskew.sort

import org.apache.spark.common.ConfigurableOrderedRDDFunctions
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.math.{pow, sqrt}
import scala.reflect.ClassTag

object HashPartitionSort {
  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]): ConfigurableOrderedRDDFunctions[K, V, (K, V)] = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val map_parallelism = args(2).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("HashPartitionSort")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    // 记录开始时间
    val startTime = System.currentTimeMillis()
    val lines: RDD[String] = sc.textFile(inputPath, map_parallelism)
    // 使用flatMap来分割单词，并按照空格、点和逗号分割
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    // 使用默认分区器
    val partitioner: HashPartitioner = new HashPartitioner(parallel)
    val sorted = wordToOne.sortByKeyWithPartitioner(partitioner = partitioner)
    val result = sorted.map(_._1)

    // 统计每个分区的单词数量
    val partitionCounts: RDD[(Int, Int)] = sorted.mapPartitionsWithIndex { (index, iterator) =>
      // 初始化分区中单词计数的总和
      val count = iterator.foldLeft(0)((sum, tuple) => sum + tuple._2)
      // 返回分区索引和该分区中单词计数的总和
      Iterator((index, count))
      //      Iterator((index, iterator.size))
    }

    // 收集每个分区的数据量
    val counts: Array[(Int, Int)] = partitionCounts.collect()

    // 计算均值和标准差
    val dataSizes = counts.map(_._2)
    val mean = dataSizes.sum.toDouble / dataSizes.length
    val variance = dataSizes.map(size => pow(size - mean, 2)).sum / dataSizes.length
    val standardDeviation = sqrt(variance)

    // 计算变异系数
    val coefficientOfVariation = if (mean != 0) standardDeviation / mean else 0
    println(s"coefficient: $coefficientOfVariation")

    val endTime = System.currentTimeMillis()
    println("Job's execute time:" + (endTime - startTime) + "ms")
    println(s"运行时间: ${(endTime - startTime) / 1000.0} s")
    result.saveAsTextFile(outputPath)

    // 关闭spark连接
    sc.stop()
  }
}
