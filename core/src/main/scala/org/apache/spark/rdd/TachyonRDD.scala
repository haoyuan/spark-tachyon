package org.apache.spark

import collection.JavaConverters._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD

import tachyon.client._

class TachyonRDDPartition(val rddId: Int, fileId: Int, val locations: Seq[String])
  extends Partition {

  override val index: Int = fileId
}

class TachyonRDD[T: ClassManifest](
    @transient sc: SparkContext,
    val files: java.util.List[java.lang.Integer])
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    val tachyonFS = SparkEnv.get.tachyonFS
    val array = new Array[Partition](files.size())
    // val locations = tachyonFS.getFilesHosts(files);
    for (i <- 0 until files.size()) {
      array(i) = new TachyonRDDPartition(id, files.get(i), Nil)//locations.get(i).asScala)
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext) = new Iterator[T] {
    val tachyonFS = SparkEnv.get.tachyonFS
    val fileId = theSplit.asInstanceOf[TachyonRDDPartition].index
    val file = tachyonFS.getFile(fileId)
    // file.open(tachyon.client.OpType.READ_TRY_CACHE)
    var tachyonBuf = file.readByteBuffer()
    if (tachyonBuf == null) {
      file.recache()
      tachyonBuf = file.readByteBuffer()
    }
    val buf = tachyonBuf.DATA
    val tachyonSerializer = SparkEnv.get.tachyonSerializer.newInstance()

    override def hasNext: Boolean = {
      buf.hasRemaining
    }

    override def next: T = {
      if (!buf.hasRemaining) {
        throw new NoSuchElementException("End of stream")
      }
      val ret : T = tachyonSerializer.deserialize[T](buf)
      ret
    }

    private def close() {
      try {
        tachyonBuf.close()
      } catch {
        case e: Exception => logWarning("Exception in TachyonFile.close()", e)
      }
    }
  }

  // override def getPreferredLocations(split: Partition): Seq[String] = {
  //   // println("***** TachyonRDD loc: " + split.asInstanceOf[TachyonRDDPartition].locations)
  //   split.asInstanceOf[TachyonRDDPartition].locations
  // }
}