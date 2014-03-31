package org.apache.spark

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import tachyon.client.TachyonFS

object TachyonRecompute {
  def main(args: Array[String]) {
    if (args.length < 3) {
      // TODO: Add program id parameter to let Tachyon know the re-computation program is running.
      System.err.println("Usage: TachyonRecompute <Host> <TachyonAddress> " +
          "<DependencyId> [<RecomputeFilesIndices>]")
      System.exit(1)
    }

    System.setProperty("spark.tachyon.address", args(1))
    val tachyonFS = TachyonFS.get(args(1))
    val dependency = tachyonFS.getClientDependencyInfo(args(2).toInt)
    val sparkContext = new SparkContext(args(0), "Recomputing dependency " + args(2))

    System.setProperty("spark.tachyon.recompute", "true")
    System.setProperty("spark.cores.max", "84")

    val WARMUP_NUM = 10
    val warm = sparkContext.parallelize(1 to WARMUP_NUM, WARMUP_NUM).map(i => {
        var sum = 0
        for (i <- 0 until WARMUP_NUM) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    val rdd = sparkContext.env.closureSerializer.newInstance().deserialize[RDD[_]](dependency.data.get(0))
    rdd.resetSparkContext(sparkContext)
    val arraybuffer = new ArrayBuffer[Int]()
    for (i <- 3 until args.length) {
      arraybuffer.append(args(i).toInt)
    }
    // rdd.tachyonRecompute(dependency, arraybuffer)
    rdd.saveAsTextFileTachyonRecompute(args(1), args(2).toInt, arraybuffer)

    System.exit(1)
  }
}