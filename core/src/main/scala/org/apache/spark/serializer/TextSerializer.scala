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

package org.apache.spark.serializer

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.util.ByteBufferInputStream

import org.apache.hadoop.io.Text

private[spark] class TextSerializationStream(out: OutputStream) extends SerializationStream {
  val dataOutput = new DataOutputStream(out)
  def writeObject[T](t: T): SerializationStream = { t.asInstanceOf[Text].write(dataOutput); this }
  def flush() { dataOutput.flush() }
  def close() { dataOutput.close() }
}

private[spark] class TextDeserializationStream(in: InputStream)
extends DeserializationStream {
  val text = new Text()
  val dataInput = new DataInputStream(in)

  def readObject[T](): T = {
    text.readFields(dataInput).asInstanceOf[T]
  }
  def close() { dataInput.close() }
}

private[spark] class TextSerializerInstance extends SerializerInstance {
  def serialize[T](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new TextSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new TextDeserializationStream(s)
  }
}

/**
 * A Spark serializer that uses Text's built-in serialization.
 */
class TextSerializer extends Serializer {
  def newInstance(): SerializerInstance = new TextSerializerInstance
}
