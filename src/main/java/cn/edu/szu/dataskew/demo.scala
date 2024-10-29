package cn.edu.szu.dataskew

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.partitioner.RTLBPA
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, StageInfo}
import org.apache.spark.sql.SparkSession

import java.util.Date

object demo {
  def main(args: Array[String]): Unit = {
    // 配置spark环境
    val sparkConf = new SparkConf().setAppName("Zipf Data Partitioner").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val filepath = "D:\\ResearchWorks\\dataSkewOptimization\\input\\zip-f\\zipf_skewness_0.1.txt"
    val data = sc.textFile(filepath)
    val partitionedData = data.repartition(100)
    partitionedData.saveAsTextFile("D:\\ResearchWorks\\dataSkewOptimization\\input\\zip-f\\skewness=0.1")

    // 关闭spark连接
    sc.stop()
  }
}
