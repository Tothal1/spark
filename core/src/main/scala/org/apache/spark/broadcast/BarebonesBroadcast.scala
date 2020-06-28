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

import io.netty.buffer.Unpooled
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.NettyManagedBuffer
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

class BarebonesBroadcast[T: ClassTag](obj: T, id: Long)
extends Broadcast[T](id) with Logging with Serializable {
  // if the value is local, I can just do return value
  // otherwise I can get it from the block manager
  private val broadcastId = BroadcastBlockId(id)

  // transient here means that even though the
  // object itself can be serialized, it will not store the value,
  // this is wanted because we already store the value in the block manager
  @transient private lazy val value_ : T = readBroadcastBlocks()


  private val numBlocks: Int = writeBroadcastBlocks(obj)
  // writeBroadcastValue(obj) send the value only
  override protected def getValue() = {
    value_
  }

  private def pushInitialBlocks(blocks: Array[ByteBuffer]) = {
    var i = 0
    val bm = SparkEnv.get.blockManager
    val peers = bm.getPeers(true) // get peer nodes so we can send the blocks to them
    val bt = bm.blockTransferService

    // for each node send a piece
    peers.foreach( peer => {
      if (i >= blocks.size) {
        i = 0;
      }
      val pieceId = BroadcastBlockId(id, "block" + i)
      // val buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(blocks(i)));
      val buffer = new NettyManagedBuffer(Unpooled.copiedBuffer(blocks(i)))
      // val buffer = new NioManagedBuffer(blocks(i))
      bt.uploadBlockSync(
          peer.host,
          peer.port,
          peer.executorId,
          pieceId,
          buffer,
          StorageLevel.MEMORY_AND_DISK,
          scala.reflect.classTag[ByteBuffer]
          )
    i = i + 1}
    )
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
  }

  private def writeBroadcastValue(value: T) {
    logInfo("writing broadcast value to block manager")
    val bm = SparkEnv.get.blockManager
    val err = bm.putSingle(broadcastId, obj, StorageLevel.MEMORY_AND_DISK, true)
    if (err == false) {
      throw new SparkException("couldn't save broadcast value or block already stored")
    }
  }

  private def writeBroadcastBlocks(value: T): Int = {
    import StorageLevel._
    val bm = SparkEnv.get.blockManager
    val startTimeMs = System.currentTimeMillis
    val blockSize = 1024 * 1024 * 4
    if (!bm.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
      throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, None)
    blocks.zipWithIndex.foreach { case (block, i) =>
      val pieceId = BroadcastBlockId(id, "block" + i)
      val bytes = new ChunkedByteBuffer(block.duplicate())
      if (!bm.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, true)) {
        throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
      }
    }
    pushInitialBlocks(blocks)
    logInfo("Writing broadcast blocks with barebones took " + Utils.getUsedTimeMs(startTimeMs))
    blocks.length
  }


  // this piece of code will not guarantee that the value will be stored locally for future use
  private def readBroadcastBlocks(): T = Utils.tryOrIOException {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val startTimeMs = System.currentTimeMillis()
    val bm = SparkEnv.get.blockManager

     bm.getLocalValues(broadcastId) match {
      case Some(blockResult) =>
        if (blockResult.data.hasNext) {
          val x = blockResult.data.next().asInstanceOf[T]
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))
          x
        }
        case None =>
      }
    val blocks = new Array[BlockData](numBlocks)
    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "block" + pid)
      logDebug(s"Reading block $pieceId of $broadcastId with barebones")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager with barebones")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId with barebones")
          }
      }
    }
    logInfo("Started reading broadcast variable " + id + "with barebones")
    logInfo("Reading broadcast variable " + id + " took" +
                Utils.getUsedTimeMs(startTimeMs) + "with barebones")
    val obj = TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, None)
    if (!bm.putSingle(broadcastId, obj, StorageLevel.MEMORY_AND_DISK, tellMaster = false)) {
                throw new SparkException(s"Failed to store $broadcastId in BlockManager")
    }

    obj
  }

  override protected def doUnpersist(blocking: Boolean) {
    // val bm = SparkEnv.get.blockManager
    // bm.removeBlock(BroadcastBlockId(id), true)
    SparkEnv.get.blockManager.master.removeBroadcast(id, false, blocking)
  }

  override protected def doDestroy(blocking: Boolean) {
    // val bm = SparkEnv.get.blockManager
    // bm.removeBlock(BroadcastBlockId(id), true)
    SparkEnv.get.blockManager.master.removeBroadcast(id, true, blocking)
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
