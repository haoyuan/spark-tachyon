/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import scala.math.random
import org.apache.spark._
import SparkContext._

/** Basic Perf **/
object BasicPerf {
  def compute(sc: SparkContext, args: Array[String]) {
    val outputdata = args(1) + "-" + args(2) + "-" + args(3)
    //  Warm up.
    val WARMUP_NUM = 500
    println("Starting warm up.")
    val warm = sc.parallelize(1 to WARMUP_NUM, WARMUP_NUM).map(i => {
        var sum = 0
        for (i <- 0 until 1) {
          sum += i
        }
        sum
      }).collect()
    println("Just warmed up.")

    val data = sc.textFile(args(1))
    if (args(3).equals("Tachyon")) {
    } else if (args(3).equals("SerCache")) {
      data.persist()
    } else if (args(3).equals("DeCache")) {
      data.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
    } else {
      System.err.println("Usage: BasicPerf <MasterAddr> <DataPath> <Count|Grep|WC]> <Tachyon|SerCache|DeCache>")
      System.err.println("Wrong Cache Type: " + args(3))
      System.exit(1)
    }
    data.count()
    // Done setup

    val times = 3
    val firstStartTimeMs = System.currentTimeMillis()
    var startTimeMs = System.currentTimeMillis()
    if (args(2).equals("Count")) {
      for (i <- 1 to times) {
        startTimeMs = System.currentTimeMillis()
        println(data.count())
        System.out.println("Round Time " + i + " : " + (System.currentTimeMillis() - startTimeMs) + " ms. " + outputdata)
      }
      System.out.println("Final Round Time " + " : " + (System.currentTimeMillis() - firstStartTimeMs) + " ms." + outputdata)
    } else if (args(2).equals("Grep")) {
      for (i <- 1 to times) {
        startTimeMs = System.currentTimeMillis()
        println(data.filter(line => line.contains("berkeley")).count())
        System.out.println("Round Time " + i + " : " + (System.currentTimeMillis() - startTimeMs) + " ms. " + outputdata)
      }
      System.out.println("Final Round Time " + " : " + (System.currentTimeMillis() - firstStartTimeMs) + " ms." + outputdata)
    } else if (args(2).equals("WC")) {
      for (i <- 1 to times) {
        startTimeMs = System.currentTimeMillis()
        data.flatMap(line => line.split(" ")).map(word => (word, i)).reduceByKey(_ + _).saveAsTextFile(outputdata + "-" + i)
        System.out.println("Round Time " + i + " : " + (System.currentTimeMillis() - startTimeMs) + " ms." + outputdata)
      }
      System.out.println("Final Round Time " + " : " + (System.currentTimeMillis() - firstStartTimeMs) + " ms." + outputdata)
    } else {
      System.err.println("Usage: BasicPerf <MasterAddr> <DataPath> <Count|Grep|WC]> <Tachyon|SerCache|DeCache>")
      System.err.println("Wrong Application Type: " + args(2))
      System.exit(1)
    }
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: BasicPerf <MasterAddr> <DataPath> <Count|Grep|WC]> <Tachyon|SerCache|DeCache>")
      System.exit(1)
    }

    // Setup
    val outputdata = args(1) + "-" + args(2) + "-" + args(3)
    val sc = new SparkContext(args(0), "BasicPerf " + outputdata,
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    System.exit(0)
  }
}
