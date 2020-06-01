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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer
import java.util.zip.Adler32

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

class BarebonesBroadcast[T: ClassTag](obj: T, id: Long)
extends Broadcast[T](id) with Logging with Serializable {
  // if the value is local, I can just do return value
  // otherwise I can get it from the block manager
  private val broadcastId = BroadcastBlockId(id)

  @transient private lazy val value_ : T = readBroadcastValue(broadcastId)


  writeBroadcastValue(obj)
  override protected def getValue() = {
    value_
  }

  private def readBroadcastValue(broadcastId: BroadcastBlockId): T = Utils.tryOrIOException {
    logInfo("Started reading broadcast variable " + id)
    val bm = SparkEnv.get.blockManager
    val startTimeMs = System.currentTimeMillis()
    // this gets the value either remotely or locally
    // this can fail
    bm.get(broadcastId) match {
      case Some(blockResult) =>
        if (blockResult.data.hasNext) {
          val x = blockResult.data.next().asInstanceOf[T]
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))
          x
        } else {
          throw new SparkException("blockResult has no next") // this should never happen
        }
      case None =>
        throw new SparkException("Value was not found either locally or remotely")
    }
    //  return value
    //   private[spark] class BlockResult(
    //   val data: Iterator[Any],
    //   val readMethod: DataReadMethod.Value,
    //   val bytes: Long)
  }

  private def writeBroadcastValue(value: T) {
    val bm = SparkEnv.get.blockManager
    val err = bm.putSingle(broadcastId, obj, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    if (err == false) {
      throw new SparkException("couldn't save broadcast value or block already stored")
    }
  }

  override protected def doUnpersist(blocking: Boolean) {
    val bm = SparkEnv.get.blockManager
    bm.removeBlock(BroadcastBlockId(id), true)
  }

  override protected def doDestroy(blocking: Boolean) {
    val bm = SparkEnv.get.blockManager
    bm.removeBlock(BroadcastBlockId(id), true)
  }
}


class BarebonesBroadcastFactory extends BroadcastFactory {
  override def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) { }
  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long): Broadcast[T]
  = new BarebonesBroadcast[T](value_, id)
  override def stop() { }
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    SparkEnv.get.blockManager.removeBlock(BroadcastBlockId(id), true)
  }
}
