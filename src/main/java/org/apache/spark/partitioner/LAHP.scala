package org.apache.spark.partitioner

import org.apache.spark.{HashPartitioner, Partitioner, TaskContext}

import scala.util.Random
import scala.collection.mutable
import scala.reflect.ClassTag

class LAHP[K: ClassTag, V](partitions: Int, threshold: Double, f: Int) extends Partitioner {

  // Initialize action set and probability vector
  private val actions: Array[Int] = (0 until partitions).toArray
  private val p: Array[Double] = Array.fill(partitions)(1.0 / partitions)
  private val RC: Array[Double] = Array.fill(partitions)(1.0) // Assuming homogeneous reducers for simplicity
  private val MapHash2LAHP: Array[Int] = actions.clone()
  private val globalCounters: Array[Double] = Array.fill(partitions)(0.0)
  private val localCounters: mutable.Map[K, Int] = mutable.Map.empty[K, Int]

  def numPartitions: Int = partitions

  // Method to select a reducer based on current probability vector
  private def selectReducer(activeReducers: Array[Int], updatedProb: Array[Double]): Int = {
    val rnd: Double = Random.nextDouble()
    var cumProb: Double = 0.0
    for (i <- activeReducers.indices) {
      cumProb += updatedProb(i)
      if (rnd < cumProb) return activeReducers(i)
    }
    activeReducers.last
  }

  // Update probabilities based on reward or penalty
  private def updateProbabilities(activeReducers: Array[Int], reducerId: Int, s: Boolean): Unit = {
    val a: Double = 0.1
    val b: Double = 0.1
    for (j <- activeReducers) {
      if (j == reducerId) {
        if (s) {
          p(j) = p(j) * (1 - b)
        } else {
          p(j) = p(j) + a * (1 - p(j))
        }
      } else {
        if (s) {
          p(j) = p(j) * (1 - b) + b / (activeReducers.length - 1)
        } else {
          p(j) = p(j) - a * p(j)
        }
      }
    }
  }

  // Rescale probabilities according to VLA Eq. (3)
  private def rescaleProbabilities(activeReducers:Array[Int], N: Double): Array[Double] = {
    val updatedProb: Array[Double] = new Array[Double](partitions)
    for (i <- actions.indices) {
      if (activeReducers.contains(i)) {
        updatedProb(i) = p(i) / N
      } else {
        updatedProb(i) = 0.0
      }
    }
    updatedProb
  }

  // Re-enable removed actions according to Eq. (4)
  private def reEnableActions(activeReducers:Array[Int], N: Double, updatedProb: Array[Double]): Unit = {
    for (i <- actions.indices) {
      if (activeReducers.contains(i)) {
        p(i) = updatedProb(i) * N
      }
    }
  }

  // Cost function to calculate the load cost
  private def cost(key: K, value: V, m: Int, x: Int): Double = {
    // Placeholder for an actual cost computation
    1.0
  }

  // Get the partition for a given key
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    val hashPartitioner: HashPartitioner = new HashPartitioner(partitions)
    val reducerId: Int = MapHash2LAHP(hashPartitioner.getPartition(k))
    // Calculate total load
    val totalLoad: Double = globalCounters.sum

    // Calculate RP for each reducer
    val RP: Array[Double] = Array.tabulate(partitions)(i => totalLoad * RC(i) / RC.sum)

    val m = TaskContext.getPartitionId()
    val kc: Int = localCounters.getOrElseUpdate(k, 0) + 1
    localCounters(k) = kc

    if (kc % f == 0) {
      if (globalCounters(reducerId) >= (1 + threshold) * RP(reducerId)) {
        // Remove overloaded reducers from action set
        val activeReducers: Array[Int] = actions.filter(i => globalCounters(i) < (1 + threshold) * RP(i))

        // Update probabilities according to VLA Eq. (3)
        val N = activeReducers.map(p(_)).sum
        val updatedProb: Array[Double] = rescaleProbabilities(activeReducers, N)

        // Select a new reducer
        val newReducerId: Int = selectReducer(activeReducers, updatedProb)

        // Calculate ALRTP
        val ALRTP: Double = activeReducers.map(i => globalCounters(i) / RP(i)).sum / activeReducers.length

        // Update probabilities based on ALRTP comparison
        if (globalCounters(reducerId) / RP(reducerId) < ALRTP) {
          updateProbabilities(activeReducers, newReducerId, s = false) // Reward
        } else {
          updateProbabilities(activeReducers, newReducerId, s = true) // Penalty
        }

        MapHash2LAHP(hashPartitioner.getPartition(k)) = newReducerId
        // Re-enable removed actions
        reEnableActions(activeReducers, N, updatedProb)

        // Update global counters with the cost function
        globalCounters(newReducerId) += cost(k, null.asInstanceOf[V], m, newReducerId)
        newReducerId
      } else {
        MapHash2LAHP(hashPartitioner.getPartition(k)) = reducerId
        // Update global counters with the cost function
        globalCounters(reducerId) += cost(k, null.asInstanceOf[V], m, reducerId)
        reducerId
      }
    } else {
      if (globalCounters(reducerId) < (1 + threshold) * RP(reducerId)) {
        MapHash2LAHP(hashPartitioner.getPartition(k)) = reducerId
        // Update global counters with the cost function
        globalCounters(reducerId) += cost(k, null.asInstanceOf[V], m, reducerId)
        reducerId
      } else {
        val newReducerId = MapHash2LAHP(hashPartitioner.getPartition(k))
        // Update global counters with the cost function
        globalCounters(newReducerId) += cost(k, null.asInstanceOf[V], m, newReducerId)
        newReducerId
      }
    }
  }
}



