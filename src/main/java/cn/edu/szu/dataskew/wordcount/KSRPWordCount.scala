package cn.edu.szu.dataskew.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.KSRP
import org.apache.spark.rdd.RDD

import scala.math.sqrt

object KSRPWordCount {
  def main(args: Array[String]): Unit = {
    val map_parallelism = args(1).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("KSRPWordCount")
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
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word: String) => (word, 1))
    val mapEndTime = System.currentTimeMillis()
    println("Map Time: " + (mapEndTime - mapStartTime) + "ms")
    // 使用自定义分区器
    val partitioner: KSRP[String, Int] = new KSRP[String, Int](parallel, wordToOne)
    // groupByKey 和 reduceByKey 操作
    val reduceStartTime = System.currentTimeMillis()
    // reduce之前先进行repartition，以确保数据均匀分布
    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(partitioner)
    // 计算每个单词的总数（局部聚合）
    val wordToCount: RDD[(String, Int)] = wordToGroup.mapValues(iter => iter.sum)
    // 全局聚合
    val result: RDD[(String, Int)] = wordToCount.reduceByKey(_+_)
    val reduceEndTime = System.currentTimeMillis() // End time for the Reduce stage
    val jobEndTime = System.currentTimeMillis() // End time for the entire job
    println("Reduce Time: " + (reduceEndTime - reduceStartTime) + "ms")
    println(s"Total Job Execute Time: ${jobEndTime - jobStartTime}" + "ms")

    val partitionLengths: Array[(Int, Int)] = result.mapPartitionsWithIndex {
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

//    println("统计结果：")
//    result.collect().foreach(println)

    sc.stop()
  }
}
