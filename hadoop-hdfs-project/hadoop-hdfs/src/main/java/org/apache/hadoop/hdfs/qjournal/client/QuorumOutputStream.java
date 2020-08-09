/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.EditsDoubleBuffer;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * EditLogOutputStream implementation that writes to a quorum of
 * remote journals.
 */
class QuorumOutputStream extends EditLogOutputStream {
  private final AsyncLoggerSet loggers;
  private EditsDoubleBuffer buf;
  private final long segmentTxId;
  private final int writeTimeoutMs;

  public QuorumOutputStream(AsyncLoggerSet loggers,
      long txId, int outputBufferCapacity,
      int writeTimeoutMs) throws IOException {
    super();
    this.buf = new EditsDoubleBuffer(outputBufferCapacity);
    this.loggers = loggers;
    this.segmentTxId = txId;
    this.writeTimeoutMs = writeTimeoutMs;
  }

  @Override
  public void write(FSEditLogOp op) throws IOException {
    buf.writeOp(op);
  }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    buf.writeRaw(bytes, offset, length);
  }

  @Override
  public void create(int layoutVersion) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    if (buf != null) {
      buf.close();
      buf = null;
    }
  }

  @Override
  public void abort() throws IOException {
    QuorumJournalManager.LOG.warn("Aborting " + this);
    buf = null;
    close();
  }

  @Override
  public void setReadyToFlush() throws IOException {
    buf.setReadyToFlush();
  }

  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    int numReadyBytes = buf.countReadyBytes();//获取元数据大小(已经处于ready状态的buf的大小)
    if (numReadyBytes > 0) {
      int numReadyTxns = buf.countReadyTxns();//获取准备flush的事务数
      long firstTxToFlush = buf.getFirstReadyTxId();//拿到准备flush的第一个事务ID

      assert numReadyTxns > 0;

      /**
       *  做一份元数据的副本，主要是高并发的异步操作，防止在写数据的时候造成这块儿数据改变，所以先写到一个buf副本
       * */
      DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
      buf.flushTo(bufToSend);//刷到副本buf
      //校验这个元数据的buf副本
      assert bufToSend.getLength() == numReadyBytes;
      byte[] data = bufToSend.getData();
      assert data.length == bufToSend.getLength();//校验

      //TODO 通过AsyncLogger，将这些edit发送到远程，异步方式，返回一个获取结果的类似Future的句柄
      QuorumCall<AsyncLogger, Void> qcall = loggers.sendEdits(
          segmentTxId, firstTxToFlush,
          numReadyTxns, data);
      //阻塞，等待journalNode的处理结果（n/2 + 1）
      loggers.waitForWriteQuorum(qcall, writeTimeoutMs, "sendEdits");
      /**
       * zookeeper 、 journalNode 奇数台
       *
       * 3 (1.5)         4 (2)
       * 2 > 1.5         3 > 2
       * 1 > 1.5 ?       2 > 2 ?
       *
       * */
      
      // Since we successfully wrote this batch, let the loggers know. Any future
      // RPCs will thus let the loggers know of the most recent transaction, even
      // if a logger has fallen behind.
      loggers.setCommittedTxId(firstTxToFlush + numReadyTxns - 1);
    }
  }

  @Override
  public String generateReport() {
    StringBuilder sb = new StringBuilder();
    sb.append("Writing segment beginning at txid " + segmentTxId + ". \n");
    loggers.appendReport(sb);
    return sb.toString();
  }
  
  @Override
  public String toString() {
    return "QuorumOutputStream starting at txid " + segmentTxId;
  }
}
