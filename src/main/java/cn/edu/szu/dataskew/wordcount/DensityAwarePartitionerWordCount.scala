package cn.edu.szu.dataskew.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.DensityAwarePartitioner
import org.apache.spark.rdd.RDD

import scala.math.sqrt

object DensityAwarePartitionerWordCount {
  def main(args: Array[String]): Unit = {
    val map_parallelism = args(1).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("DensityAwarePartitionerWordCount")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    val jobStartTime = System.currentTimeMillis()
    val filepath = args(0)
    val lines: RDD[String] = sc.textFile(filepath, map_parallelism)
    val mapStartTime = System.currentTimeMillis()
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    val mapEndTime = System.currentTimeMillis()
    println("Map Time: " + (mapEndTime - mapStartTime) + "ms")
    // 使用自定义分区器
    val partitioner: DensityAwarePartitioner[String, Int] = new DensityAwarePartitioner[String, Int](parallel, wordToOne)
    val reduceStartTime = System.currentTimeMillis() // End time for the Map stage
    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(partitioner)
    val reduceEndTime = System.currentTimeMillis() // End time for the Reduce stage
    // 计算每个单词的总数
    val wordToCount: RDD[(String, Int)] = wordToGroup.mapValues(iter => iter.sum)
    // 全局聚合
    val result: RDD[(String, Int)] = wordToCount.reduceByKey(_+_)
    val jobEndTime = System.currentTimeMillis() // End time for the entire job
    println("Reduce Time: " + (reduceEndTime - reduceStartTime) + "ms")
    println(s"Total Job Execute Time: ${jobEndTime - jobStartTime}" + "ms")

    val partitionLengths: Array[(Int, Int)] = result.mapPartitionsWithIndex {
      (index, data) => {
        var len = 0
        while (data.hasNext) {
          len += data.next()._2
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
