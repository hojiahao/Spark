package cn.edu.szu.dataskew.wordcount

import org.apache.spark.partitioner.RTLBPA
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Date

object RTLBPA2WordCount {
  def main(args: Array[String]): Unit = {
    val numMappers = 24
    val numReducers = 16

    // 配置spark环境
    val spark = SparkSession
      .builder
      .master("local")
      .appName(s"BTS_WordCount_RTLBPA")
      .config("spark.submit.deployMode","cluster")
      .config("spark.default.parallelism", "400")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .config("spark.cores.max", "24")
      .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
      .getOrCreate()

    //    val filepath=args(0)
    val filepath = args(0)
    val lines: RDD[String] = spark.sparkContext.textFile(filepath, numMappers)
    val map_startTime = new Date().getTime
    // 使用flatMap来分割单词，使用正则表达式 "[\\s,.\n\t?!]+"是一个正则表达式，用于匹配空白字符、逗号、点、换行符、制表符、问号和感叹号。）
    val words: RDD[String] = lines.flatMap(_.split("[\\s,.:\n\t?!\\d]+"))
    // 映射单词为键值对(word, 1)
    val wordToOne: RDD[(String, Int)] = words.map((word:String) => (word, 1))
    // 使用自定义分区器
    val RTLBPApartitioner = new RTLBPA(numReducers)
    // reduce之前先进行repartition，以确保数据均匀分布
    val wordToGroup: RDD[(String, Iterable[Int])] = wordToOne.groupByKey(RTLBPApartitioner)
    // 计算每个单词的总数
    val wordToCount = wordToGroup.map(word_iter => (word_iter._1, word_iter._2.sum))
    wordToCount.collect()
    val reduce_endTime = new Date().getTime
    // 输出Map阶段的分区个数
    println("Map分区个数：" + wordToOne.getNumPartitions)
    // 输出Reduce阶段的分区个数
    println("Reduce分区个数：" + wordToCount.getNumPartitions)
    println("Reduce时间为：" + (reduce_endTime - map_startTime) + "毫秒")

    // 输出每个分区中的数据量
    println("Reduce分区情况：")
    val partitionLengths: Array[(Int, Int)] = wordToCount.mapPartitionsWithIndex {
      (index, data) => {
        var len = 0
        while (data.hasNext) {
          len += data.next()._2
        }
        Iterator((index, len))
      }
    }.collect()

    partitionLengths.foreach { case (index, len) => println(index + "----->" + len) }

    println("统计结果：")
    wordToCount.collect().foreach(println)

    // 关闭spark连接
    spark.stop()
  }
}
