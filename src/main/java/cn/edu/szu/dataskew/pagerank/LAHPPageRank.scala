package cn.edu.szu.dataskew.pagerank

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.partitioner.LAHP
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd, StageInfo}
import org.apache.spark.sql.SparkSession

import java.util.Date
import scala.math.sqrt

object LAHPPageRank {

  var totalCpuTime: Long = 0L
  var totalMemoryUsage: Long = 0L
  var totalDiskUsage: Long = 0L

  def main(args: Array[String]): Unit = {
    val numMappers = 400 // 映射器数量
    val numReducers = 400 // 聚合器数量
    val threshold = args(3).toDouble // 阈值
    val f = args(4).toInt  // 步长

    // 配置spark环境
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName(s"LAHPWordCount")
      .config("spark.submit.deployMode","cluster")
      .config("spark.default.parallelism", "400")
      .config("spark.driver.memory", "4g")
      .config("spark.driver.cores", "8")
      .config("spark.executor.cores", "8")
      .config("spark.executor.memory", "10g")
      .config("spark.num.executors", "12")
      .config("spark.cores.max", "96")
      .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .getOrCreate()

    // 记录开始时间
    val jobStartTime = new Date().getTime

    //    val filepath = args(0)
    val filepath = args(0)
    val outputpath = args(1)
    val iters = if (args.length > 2) args(2).toInt else 10
    val lines: RDD[String] = spark.sparkContext.textFile(filepath, numMappers)

    val mapStartTime = new Date().getTime // Start time for the Map stage
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val links = lines.map({ s =>
      val elements = s.split("[\\s,.:\n\t?!\\d]+")
      val parts = elements.slice(elements.length - 2, elements.length)
      (parts(0), parts(1))
    }).distinct()

    val mapEndTime = new Date().getTime // End time for the Map stage
    // 使用自定义分区器
    val partitioner: LAHP[String, Int] = new LAHP[String, Int](numReducers, threshold, f)
    val reduceStartTime = new Date().getTime // End time for the Map stage
    val linksToGroup = links.groupByKey(partitioner).cache()
    var ranks = linksToGroup.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = linksToGroup.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    val reduceEndTime = new Date().getTime // End time for the Reduce stage
    val jobEndTime = new Date().getTime // End time for the entire job

    val mapTime = (mapEndTime - mapStartTime) / 1e3
    val reduceTime = (reduceEndTime - reduceStartTime) / 1e3
    val executionTime = (jobEndTime - jobStartTime) / 1e3

    // 统计资源消耗（使用Spark的UI或日志来查看资源消耗情况）
    val sc: SparkContext = spark.sparkContext
    sc.addSparkListener(new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        // Accumulate CPU time (in nanoseconds)
        totalCpuTime += taskEnd.taskMetrics.executorCpuTime
        // Accumulate memory usage (in bytes)
        totalMemoryUsage += taskEnd.taskMetrics.peakExecutionMemory
        // Accumulate disk usage (in bytes, used for shuffling)
        totalDiskUsage += taskEnd.taskMetrics.diskBytesSpilled
      }
    })

    // Compute partition sizes for COV calculation
    val partitionSizes = links.mapPartitions(iter => Iterator(iter.size)).collect()
    // Compute average size of partitions
    val mean = partitionSizes.sum.toDouble / partitionSizes.length
    // Compute standard deviation
    val variance = partitionSizes.map(size => math.pow(size - mean, 2)).sum / partitionSizes.length
    val stddev = sqrt(variance)
    // Compute COV (Coefficient of Variation)
    val cov = stddev / mean

    // Convert CPU time to seconds and print the results
    val totalCpuTimeInSeconds = totalCpuTime / 1e9
    println(s"Total CPU time: $totalCpuTimeInSeconds seconds")

    // Convert memory usage to MB and print the results
    val totalMemoryUsageInMB = totalMemoryUsage / (1024 * 1024)
    println(s"Total memory usage: $totalMemoryUsageInMB MB")

    // Convert disk usage to MB and print the results
    val totalDiskUsageInMB = totalDiskUsage / (1024 * 1024)
    println(s"Total disk usage: $totalDiskUsageInMB MB")

    val indicators = sc.parallelize(Seq(
      s"Map time: $mapTime",
      s"Reduce time: $reduceTime",
      s"Execution time: $executionTime",
      s"Total CPU time: $totalCpuTimeInSeconds seconds",
      s"Total memory usage: $totalMemoryUsageInMB MB",
      s"Total disk usage: $totalDiskUsageInMB MB",
      s"Coefficient of Variation (COV): $cov"
    ))

    ranks.saveAsTextFile(outputpath)
    indicators.coalesce(1).saveAsTextFile(outputpath + "indicators/")

    //    println("统计结果：")
    //    wordToCount.collect().foreach(println)

    // 关闭spark连接
    spark.stop()
  }
}
