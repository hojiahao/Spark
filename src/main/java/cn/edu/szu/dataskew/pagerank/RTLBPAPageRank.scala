package cn.edu.szu.dataskew.pagerank

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.RTLBPA
import org.apache.spark.rdd.RDD

import scala.math.sqrt

object RTLBPAPageRank {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val map_parallelism = args(2).toInt
    // 创建SparkConf对象并设置配置
    val sparkConf = new SparkConf()
      .setMaster("yarn")
      .setAppName("HashPartitionPageRank")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .set("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(sparkConf)
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    // 记录开始时间
    val startTime = System.currentTimeMillis()
    val iters = if (args.length > 3) args(3).toInt else 10
    val lines: RDD[String] = sc.textFile(inputPath, map_parallelism)
    val mapStartTime = System.currentTimeMillis() // Start time for the Map stage
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val links = lines.map({ s =>
      val elements = s.split("[\\s,.:\n\t?!\\d]+")
      val parts = elements.slice(elements.length - 2, elements.length)
      (parts(0), parts(1))
    }).distinct()
    // 使用自定义分区算法
    val partitioner: RTLBPA[String, String] = new RTLBPA[String, String](parallel)
    val linksToGroup = links.groupByKey(partitioner).cache()
    var ranks = linksToGroup.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = linksToGroup.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val endTime = System.currentTimeMillis()
    println("Job's execute time:" + (endTime - startTime) + "ms")
    println(s"运行时间: ${(endTime - startTime) / 1000.0} s")
    ranks.saveAsTextFile(outputPath)
    // 关闭spark连接
    sc.stop()
  }
}
