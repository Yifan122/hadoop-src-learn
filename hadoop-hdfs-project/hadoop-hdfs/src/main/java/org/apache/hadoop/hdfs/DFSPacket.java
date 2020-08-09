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
package org.apache.hadoop.hdfs;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.htrace.Span;

/****************************************************************
 * DFSPacket is used by DataStreamer and DFSOutputStream.
 * DFSOutputStream generates packets and then ask DatStreamer
 * to send them to datanodes.
 * 数据包是按照一组组数据块写入的，先写校验和数据块，再写真实数据块，
 * 然后再写下一组校验和数据块和真实数据块，最后再写上header头部信息，至此整个数据包写完。
 ****************************************************************/

class DFSPacket {
  // 如果长时间没有数据传输，在输出流未关闭的情况下，客户端会发送心跳包给数据节点，心跳包是DataPacket的一种特殊实现，它通过数据包序列号为-1来进行特殊标识
  public static final long HEART_BEAT_SEQNO = -1L;
  private static long[] EMPTY = new long[0];
  private final long seqno; // 序列号
  private final long offsetInBlock; // 数据在数据块中的位置
  private int numChunks; // 序列号
  private boolean syncBlock; // this packet forces the current block to disk
  private final int maxChunks; // 数据包中的最大数据块数
  private byte[] buf;//数据缓冲区
  private final boolean lastPacketInBlock; // 是否为block中最后一个数据包

  /**
   * buf is pointed into like follows:
   *  (C is checksum data, D is payload data)
   *
   * [_________CCCCCCCCC________________DDDDDDDDDDDDDDDD___]
   *           ^        ^               ^               ^
   *           |        checksumPos     dataStart       dataPos
   *           checksumStart
   *
   * Right before sending, we move the checksum data to immediately precede
   * the actual data, and then insert the header into the buffer immediately
   * preceding the checksum data, so we make sure to keep enough space in
   * front of the checksum data to support the largest conceivable header.
   */
  //4大指针
  private int checksumStart;//标记数据校验和区域起始位置
  private int checksumPos;//标记数据校验和区域当前写入位置
  private final int dataStart;//标记真实数据区域起始位置
  private int dataPos;//标记真实数据区域当前写入位置
  private long[] traceParents = EMPTY;
  private int traceParentsUsed;
  private Span span;

  /**
   * Create a new packet.
   *
   * @param buf the buffer storing data and checksums
   * @param chunksPerPkt maximum number of chunks per packet.
   * @param offsetInBlock offset in bytes into the HDFS block.
   * @param seqno the sequence number of this packet
   * @param checksumSize the size of checksum
   * @param lastPacketInBlock if this is the last packet
   */
  DFSPacket(byte[] buf, int chunksPerPkt, long offsetInBlock, long seqno,
                   int checksumSize, boolean lastPacketInBlock) {
    this.lastPacketInBlock = lastPacketInBlock;
    this.numChunks = 0;
    this.offsetInBlock = offsetInBlock;
    this.seqno = seqno;

    this.buf = buf;

    checksumStart = PacketHeader.PKT_MAX_HEADER_LEN;
    checksumPos = checksumStart;
    dataStart = checksumStart + (chunksPerPkt * checksumSize);
    dataPos = dataStart;
    maxChunks = chunksPerPkt;
  }

  /**
   * Write data to this packet.
   *往包内写入数据
   * @param inarray input array of data
   * @param off the offset of data to write
   * @param len the length of data to write
   * @throws ClosedChannelException
   */
  synchronized void writeData(byte[] inarray, int off, int len)
      throws ClosedChannelException {
    // 检测缓冲区
    checkBuffer();
    // 检测数据当前位置后如果 写入len个字节，是否会超过缓冲区大小
    if (dataPos + len > buf.length) {
      throw new BufferOverflowException();
    }
    // 数据拷贝：从数据当前位置处起开始存放len个字节
    System.arraycopy(inarray, off, buf, dataPos, len);
    // 数据当前位置累加len，指针向后移动
    dataPos += len;
  }

  /**
   * Write checksums to this packet
   *往包内写入校验和
   * @param inarray input array of checksums
   * @param off the offset of checksums to write
   * @param len the length of checksums to write
   * @throws ClosedChannelException
   */
  synchronized void writeChecksum(byte[] inarray, int off, int len)
      throws ClosedChannelException {
    // 检测缓冲区
    checkBuffer();
    // 校验数据校验和长度
    if (len == 0) {
      return;
    }
    // 检测数据当前位置后如果 写入len个字节，是否会超过缓冲区大小
    if (checksumPos + len > dataStart) {
      throw new BufferOverflowException();
    }
    // 数据拷贝：从校验和当前位置处起开始存放len个字节
    System.arraycopy(inarray, off, buf, checksumPos, len);
    // 数据当前位置累加len，指针向后移动
    checksumPos += len;
  }

  /**
   * Write the full packet, including the header, to the given output stream.
   *缓冲区数据flush到输出流
         发送数据过程：
         1、 计算数据包的数据长度；
         2、 生成头部header信息：一个protobuf对象；
         3、 整理缓冲区，去除校验和块区域 和 真实数据块区域间的空隙；
         4、 添加头部信息到缓冲区：从校验和块区域起始往前计算头部信息的起始位置；
         5、 将缓冲区数据写入到输出流。
   * @param stm
   * @throws IOException
   * 将整个数据包写入到指定流，包含头部header
   */
  synchronized void writeTo(DataOutputStream stm) throws IOException {
    // 检测缓冲区
    checkBuffer();
    // 计算数据长度
    final int dataLen = dataPos - dataStart;
    // 计算校验和长度
    final int checksumLen = checksumPos - checksumStart;
    // 计算整个包的数据长度（数据长度+校验和长度+固定长度4）
    final int pktLen = HdfsConstants.BYTES_IN_INTEGER + dataLen + checksumLen;
    // 构造数据包包头信息（protobuf对象）
    PacketHeader header = new PacketHeader(
        pktLen, offsetInBlock, seqno, lastPacketInBlock, dataLen, syncBlock);
    // 如果校验和数据当前位置不等于数据起始处，挪动校验和数据以填补空白
    if (checksumPos != dataStart) {
      // 这个可能在最后一个数据包或者一个hflush/hsyn调用时发生
      System.arraycopy(buf, checksumStart, buf,
          dataStart - checksumLen , checksumLen);
      // 重置checksumPos、checksumStart
      checksumPos = dataStart;
      checksumStart = checksumPos - checksumLen;
    }
    // 计算header的起始位置：数据块校验和起始处减去序列化后的头部大小
    final int headerStart = checksumStart - header.getSerializedSize();
    assert checksumStart + 1 >= header.getSerializedSize();
    assert headerStart >= 0;
    assert headerStart + header.getSerializedSize() == checksumStart;

    // 将header数据写入缓冲区。header是用protobuf序列化的
    System.arraycopy(header.getBytes(), 0, buf, headerStart,
        header.getSerializedSize());

    // 测试用.
    if (DFSClientFaultInjector.get().corruptPacket()) {
      buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
    }

    // 写入当前整个连续的packet至输出流
    // 从header起始处，写入长度为头部大小、校验和长度、数据长度的总和
    stm.write(buf, headerStart, header.getSerializedSize() + checksumLen + dataLen);

    // undo corruption.
    if (DFSClientFaultInjector.get().uncorruptPacket()) {
      buf[headerStart+header.getSerializedSize() + checksumLen + dataLen-1] ^= 0xff;
    }
  }

  private synchronized void checkBuffer() throws ClosedChannelException {
    if (buf == null) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Release the buffer in this packet to ByteArrayManager.
   *
   * @param bam
   */
  synchronized void releaseBuffer(ByteArrayManager bam) {
    bam.release(buf);
    buf = null;
  }

  /**
   * get the packet's last byte's offset in the block
   *
   * @return the packet's last byte's offset in the block
   */
  synchronized long getLastByteOffsetBlock() {
    return offsetInBlock + dataPos - dataStart;
  }

  /**
   * Check if this packet is a heart beat packet
   *判断该包释放为心跳包
   * @return true if the sequence number is HEART_BEAT_SEQNO
   */
  boolean isHeartbeatPacket() {
    // 心跳包的序列号均为-1
    return seqno == HEART_BEAT_SEQNO;
  }

  /**
   * check if this packet is the last packet in block
   *
   * @return true if the packet is the last packet
   */
  boolean isLastPacketInBlock(){
    return lastPacketInBlock;
  }

  /**
   * get sequence number of this packet
   *
   * @return the sequence number of this packet
   */
  long getSeqno(){
    return seqno;
  }

  /**
   * get the number of chunks this packet contains
   *
   * @return the number of chunks in this packet
   */
  synchronized int getNumChunks(){
    return numChunks;
  }

  /**
   * increase the number of chunks by one
   */
  synchronized void incNumChunks(){
    numChunks++;
  }

  /**
   * get the maximum number of packets
   *
   * @return the maximum number of packets
   */
  int getMaxChunks(){
    return maxChunks;
  }

  /**
   * set if to sync block
   *
   * @param syncBlock if to sync block
   */
  synchronized void setSyncBlock(boolean syncBlock){
    this.syncBlock = syncBlock;
  }

  @Override
  public String toString() {
    return "packet seqno: " + this.seqno +
        " offsetInBlock: " + this.offsetInBlock +
        " lastPacketInBlock: " + this.lastPacketInBlock +
        " lastByteOffsetInBlock: " + this.getLastByteOffsetBlock();
  }

  /**
   * Add a trace parent span for this packet.<p/>
   *
   * Trace parent spans for a packet are the trace spans responsible for
   * adding data to that packet.  We store them as an array of longs for
   * efficiency.<p/>
   *
   * Protected by the DFSOutputStream dataQueue lock.
   */
  public void addTraceParent(Span span) {
    if (span == null) {
      return;
    }
    addTraceParent(span.getSpanId());
  }

  public void addTraceParent(long id) {
    if (traceParentsUsed == traceParents.length) {
      int newLength = (traceParents.length == 0) ? 8 :
          traceParents.length * 2;
      traceParents = Arrays.copyOf(traceParents, newLength);
    }
    traceParents[traceParentsUsed] = id;
    traceParentsUsed++;
  }

  /**
   * Get the trace parent spans for this packet.<p/>
   *
   * Will always be non-null.<p/>
   *
   * Protected by the DFSOutputStream dataQueue lock.
   */
  public long[] getTraceParents() {
    // Remove duplicates from the array.
    int len = traceParentsUsed;
    Arrays.sort(traceParents, 0, len);
    int i = 0, j = 0;
    long prevVal = 0; // 0 is not a valid span id
    while (true) {
      if (i == len) {
        break;
      }
      long val = traceParents[i];
      if (val != prevVal) {
        traceParents[j] = val;
        j++;
        prevVal = val;
      }
      i++;
    }
    if (j < traceParents.length) {
      traceParents = Arrays.copyOf(traceParents, j);
      traceParentsUsed = traceParents.length;
    }
    return traceParents;
  }

  public void setTraceSpan(Span span) {
    this.span = span;
  }

  public Span getTraceSpan() {
    return span;
  }
}
