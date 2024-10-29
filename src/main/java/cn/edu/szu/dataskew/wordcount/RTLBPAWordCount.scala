package cn.edu.szu.dataskew.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.RTLBPA
import org.apache.spark.rdd.RDD

import scala.math.sqrt

object RTLBPAWordCount {
  def main(args: Array[String]): Unit = {
    val map_parallelism = args(1).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("RTLBPAWordCount")
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
    // 使用自定义分区器
    val partitioner = new RTLBPA(parallel)
    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(partitioner)
    // 计算每个单词的总数
    val wordToCount = wordToGroup.map(word_iter => (word_iter._1, word_iter._2.sum))
    // groupByKey 和 reduceByKey 操作
    val reduceStartTime = System.currentTimeMillis()
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

    //    println("统计结果：")
    //    wordToCount.collect().foreach(println)

    // 关闭spark连接
    sc.stop()
  }
}
