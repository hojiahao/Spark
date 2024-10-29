package cn.edu.szu.dataskew.sort

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.common.ConfigurableOrderedRDDFunctions
import org.apache.spark.partitioner.LAHP
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd, StageInfo}
import org.apache.spark.sql.SparkSession

import java.util.Date
import scala.math.sqrt
import scala.reflect.ClassTag

object LAHPSort {

  var totalCpuTime: Long = 0L
  var totalMemoryUsage: Long = 0L
  var totalDiskUsage: Long = 0L

  implicit def rddToHashedRDDFunctions[K : Ordering : ClassTag, V: ClassTag]
  (rdd: RDD[(K, V)]) = new ConfigurableOrderedRDDFunctions[K, V, (K, V)](rdd)

  def main(args: Array[String]): Unit = {
    val numMappers = 400 // 映射器数量
    val numReducers = 400 // 聚合器数量
    val threshold = args(2).toDouble // 阈值
    val f = args(3).toInt  // 步长

    // 配置spark环境
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName(s"LAHPSort")
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
    val lines: RDD[String] = spark.sparkContext.textFile(filepath, numMappers)

    val mapStartTime = new Date().getTime
    // 获取CSV最后一列
    val comments: RDD[String] = lines.map(_.split(",").last)
    // 使用flatMap来分割单词，并按照空格、点和逗号分割
    val words: RDD[String] = comments.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    val mapEndTime = new Date().getTime
    //    println("Map阶段耗时: " + (mapEndTime - mapStartTime) + "毫秒")
    // 使用自定义分区器
    val partitioner: LAHP[String, Int] = new LAHP[String, Int](numReducers, threshold, f)
    val sorted = wordToOne.sortByKeyWithPartitioner(partitioner = partitioner).map(_._1)
    // 记录结束时间
    val jobEndTime = new Date().getTime
    //    println(s"作业总耗时: ${jobEndTime - jobStartTime} 毫秒")

    val mapTime = (mapEndTime - mapStartTime) / 1e3
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

    // Get the size of each partition
    val partitionSizes = sorted.mapPartitions(iter => Iterator(iter.size)).collect()
    // Calculate the mean
    val mean = partitionSizes.sum.toDouble / partitionSizes.length
    // Calculate the standard deviation
    val variance = partitionSizes.map(size => math.pow(size - mean, 2)).sum / partitionSizes.length
    val stddev = sqrt(variance)
    // Calculate the coefficient of variation (COV)
    val cov = stddev / mean

    // Convert CPU time to seconds and print the results
    val totalCpuTimeInSeconds = totalCpuTime / 1e9
    //    println(s"Total CPU time: $totalCpuTimeInSeconds seconds")

    // Convert memory usage to MB and print the results
    val totalMemoryUsageInMB = totalMemoryUsage / (1024 * 1024)
    //    println(s"Total memory usage: $totalMemoryUsageInMB MB")

    // Convert disk usage to MB and print the results
    val totalDiskUsageInMB = totalDiskUsage / (1024 * 1024)
    //    println(s"Total disk usage: $totalDiskUsageInMB MB")

    val indicators = sc.parallelize(Seq(
      s"Map time: $mapTime",
      s"Execution time: $executionTime",
      s"Total CPU time: $totalCpuTimeInSeconds seconds",
      s"Total memory usage: $totalMemoryUsageInMB MB",
      s"Total disk usage: $totalDiskUsageInMB MB",
      s"Coefficient of Variation (COV): $cov"
    ))

    sorted.saveAsTextFile(outputpath)
    indicators.coalesce(1).saveAsTextFile(outputpath + "indicators/")

    //    println("统计结果：")
    //    wordToCount.collect().foreach(println)

    // 关闭spark连接
    spark.stop()
  }
}
