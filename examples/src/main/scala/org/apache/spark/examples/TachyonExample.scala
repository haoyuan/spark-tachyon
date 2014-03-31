package org.apache.spark.examples

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import tachyon.client.TachyonFS

object TachyonExample {
  val folder = "/TachyonExample"
  var sc : SparkContext = null

  def endToEndTest() {
    System.out.println("TachyonExample end to end test is starting...");
    SparkEnv.get.tachyonFS.delete(folder, true)
    val data = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    val pData = sc.parallelize(data)
    pData.saveToTachyon(null, folder + "/ori")

    val res = pData.filter(x => (x % 2 == 0))
    var localValue = res.collect()
    println("++++++++++++\n" + localValue.deep.mkString("\n"))
    res.saveToTachyon(null, folder + "/res")

    val tData = sc.readFromTachyon[Int](folder + "/ori")
    tData.collect().foreach(ele => {System.out.print("ORIX: " + ele + " :ori\n")})
    System.out.println("********************************")
    val tRes = sc.readFromTachyon[Int](folder + "/res")
    tRes.collect().foreach(ele => {System.out.print("RESX: " + ele + " :res\n")})
  }

  def generateBinaryCodeTest() {
    System.out.println("TachyonExample generateTestBinaryCode is starting...");
    val data = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    val pData = sc.parallelize(data)
    val dependencyId = pData.saveToTachyon(null, folder + "/ori")
    val dependency = SparkEnv.get.tachyonFS.getClientDependencyInfo(dependencyId)
    val rdd = SparkEnv.get.closureSerializer.newInstance().deserialize[RDD[_]](dependency.data.get(0))
    rdd.resetSparkContext(sc)
    val arraybuffer = new ArrayBuffer[Int]()
    for (i <- 0 until 1) {
      arraybuffer.append(i)
    }
    rdd.tachyonRecompute(dependency, arraybuffer)
  }

  def main(args: Array[String]) {
    sc = new SparkContext(args(0), "TachyonExample")
    endToEndTest();
    generateBinaryCodeTest()
    System.exit(1);
  }
}