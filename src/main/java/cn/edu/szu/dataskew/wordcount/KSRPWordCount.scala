package cn.edu.szu.dataskew.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.KSRP
import org.apache.spark.rdd.RDD

import scala.math.{pow, sqrt}

object KSRPWordCount {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)
    val map_parallelism = args(2).toInt
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

    val startTime = System.currentTimeMillis()
    val lines: RDD[String] = sc.textFile(inputPath, map_parallelism)
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    // 使用自定义分区器
    val partitioner: KSRP[String, Int]= new KSRP[String, Int](parallel, wordToOne)
    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(partitioner)
    // 计算每个单词的总数
    val wordCounts: RDD[(String, Int)] = wordToGroup.mapValues(iter => iter.sum)
    // 输出Map阶段的分区个数
    println("number of Mappers:" + wordToOne.getNumPartitions)
    // 输出Reduce阶段的分区个数
    println("number of reducers:" + wordCounts.getNumPartitions)

    // 统计每个分区的单词数量
    val partitionCounts: RDD[(Int, Int)] = wordCounts.mapPartitionsWithIndex { (index, iterator) =>
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

    //    // 打印每个分区的数据量和变异系数
    //    println("每个分区的数据量：")
    //    counts.foreach { case (partitionIndex, count) =>
    //      println(s"分区 $partitionIndex: $count")
    //    }
    println(s"coefficient: $coefficientOfVariation")

    val endTime = System.currentTimeMillis()
    println("Job's execute time:" + (endTime - startTime) + "ms")
    println(s"运行时间: ${(endTime - startTime) / 1000.0} s")
    wordCounts.saveAsTextFile(outputPath)
    //    println("result:")
    //    wordToCount.collect().foreach(println)

    // 关闭spark连接
    sc.stop()
  }
}
