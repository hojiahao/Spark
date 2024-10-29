package cn.edu.szu.dataskew.pagerank

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.math.sqrt

object RangePartitionPageRank {
  def main(args: Array[String]): Unit = {
    val map_parallelism = args(1).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("RangePartitionPageRank")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    // 记录开始时间
    val jobStartTime = System.currentTimeMillis()
    val filepath = args(0)
    val iters = if (args.length > 2) args(2).toInt else 10
    val lines: RDD[String] = sc.textFile(filepath, map_parallelism)
    val mapStartTime = System.currentTimeMillis() // Start time for the Map stage
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val links = lines.map({ s =>
      val elements = s.split("[\\s,.:\n\t?!\\d]+")
      val parts = elements.slice(elements.length - 2, elements.length)
      (parts(0), parts(1))
    }).distinct()
    val mapEndTime = System.currentTimeMillis() // End time for the Map stage
    println("Map Time: " + (mapEndTime - mapStartTime) + "ms")
    // 使用默认分区算法
    val partitioner: RangePartitioner[String, String] = new RangePartitioner[String, String](parallel, links)
    val linksToGroup = links.groupByKey(partitioner).cache()
    var ranks = linksToGroup.mapValues(v => 1.0)
    val reduceStartTime = System.currentTimeMillis() // End time for the Map stage
    for (i <- 1 to iters) {
      val contribs = linksToGroup.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    val reduceEndTime = System.currentTimeMillis() // End time for the Reduce stage
    val jobEndTime = System.currentTimeMillis() // End time for the entire job
    println("Reduce Time: " + (reduceEndTime - reduceStartTime) + "ms")
    println(s"Total Job Execute Time: ${jobEndTime - jobStartTime}" + "ms")

    val partitionLengths: Array[(Int, Int)] = ranks.mapPartitionsWithIndex {
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
    //    wordToCount.collect().foreach(println)

    // 关闭spark连接
    sc.stop()
  }
}
