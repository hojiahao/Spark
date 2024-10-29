package cn.edu.szu.dataskew.wordcount

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.partitioner.RoundRobinPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd, StageInfo}
import org.apache.spark.sql.SparkSession

import java.util.Date

object LADBPWordCount {

  var totalCpuTime: Long = 0L
  var totalMemoryUsage: Long = 0L
  var totalDiskUsage: Long = 0L

  def main(args: Array[String]): Unit = {
    val numMappers = 400 // 映射器数量
    val numReducers = 400 // 聚合器数量
    val threshold = args(2).toDouble // 阈值
    val step = args(3).toInt  // 步长
    val historyLoad = Map(0 -> 6.4, 1 -> 1.12, 2 -> 2.08, 3 -> 3.2, 4 -> 2.4, 5 -> 0.8) // Example historical load data

    // 配置spark环境
    val spark = SparkSession
      .builder
      .master("yarn")
      .appName(s"LADBPWordCount")
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

    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word: String) => (word, 1))

    val mapEndTime = new Date().getTime
//    println("Map阶段耗时: " + (mapEndTime - mapStartTime) + "毫秒")

    // 使用自定义分区器
    val partitioner: RoundRobinPartitioner[String, Int] = new RoundRobinPartitioner[String, Int](numReducers, wordToOne)

    // groupByKey 和 reduceByKey 操作
    val reduceStartTime = new Date().getTime

    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(partitioner)
    // 计算每个单词的总数（局部聚合）
    val wordToCount: RDD[(String, Int)] = wordToGroup.mapValues(iter => iter.sum)
    // 全局聚合
    val result: RDD[(String, Int)] = wordToCount.reduceByKey(_+_)

    val reduceEndTime = new Date().getTime()
//    println("Reduce阶段耗时: " + (reduceEndTime - reduceStartTime) + "毫秒")

    // 记录结束时间
    val jobEndTime = new Date().getTime
//    println(s"作业总耗时: ${jobEndTime - jobStartTime} 毫秒")

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

    val partitionLengths: Array[(Int, Int)] = wordToCount.mapPartitionsWithIndex {
      (index, data) => {
        var len = 0
        while (data.hasNext) {
          len += data.next()._2
        }
        Iterator((index, len))
      }
    }.collect()

    // 计算分区数据的方差和变异系数
    val mean = partitionLengths.map(_._2).sum.toDouble / partitionLengths.length
    val variance = partitionLengths.map({ case (_, len) => math.pow(len - mean, 2)}).sum / partitionLengths.length
    val std = math.sqrt(variance)
    val cov = std / mean

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
      s"Reduce time: $reduceTime",
      s"Execution time: $executionTime",
      s"Total CPU time: $totalCpuTimeInSeconds seconds",
      s"Total memory usage: $totalMemoryUsageInMB MB",
      s"Total disk usage: $totalDiskUsageInMB MB",
      s"Coefficient of Variation (COV): $cov"
    ))

    result.saveAsTextFile(outputpath)
    indicators.coalesce(1).saveAsTextFile(outputpath + "indicators/")

//    println("统计结果：")
//    result.collect().foreach(println)

    spark.stop()
  }
}
