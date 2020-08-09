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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.PeerServer;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;

/**
 * DataXceiverServer是数据节点DataNode上一个用于接收数据读写请求的后台工作线程，
 * 为每个数据读写请求创建一个单独的线程去处理。它提供了一请求一线程的模式，并对线程数目做了控制，
 * 对接收数据读写请求时发生的各种异常做了很好的容错处理，特别是针对内存溢出异常，允许等待短暂时间再继续提供服务，
 * 避免内存使用高峰期等等。
 * 而且，线程组与后台线程的应用，也大大简化这些线程的管理工作；
 * 对数据读写请求处理线程的数目，集群内数据块移动线程数目都做了严格控制，避免资源无节制耗费等。
 * 这些设计很好的支撑了HDFS大吞吐量数据的性能要求
 */
class DataXceiverServer implements Runnable {
  public static final Log LOG = DataNode.LOG;

  // PeerServer是一个接口，实现了它的TcpPeerServer封装了一个ServerSocket，提供了Java Socket服务端的功能
  private final PeerServer peerServer;

  // DataXceiverServer内部还存在对于其载体DataNode的实例datanode，这样该线程就能随时获得DataNode状态、提供的一些列服务等；
  private final DataNode datanode;

  // Peer所在线程的映射集合peers
  private final HashMap<Peer, Thread> peers = new HashMap<Peer, Thread>();

  // Peer与DataXceiver的映射集合peersXceiver
  private final HashMap<Peer, DataXceiver> peersXceiver = new HashMap<Peer, DataXceiver>();

  // DataXceiverServer是否已关闭的标志位closed
  private boolean closed = false;

  
  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   *
   *
   * 每个节点并行的最大DataXceivers数目。
   * 为了避免dataNode运行内存溢出，执行这个限制是必须的。
   * 定义是默认值为4096.
   */
  int maxXceiverCount =
    DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT;

  /** A manager to make sure that cluster balancing does not
   * take too much resources.
   * 
   * It limits the number of block moves for balancing and
   * the total amount of bandwidth they can use.
   *
   * 确保集群平衡不占用太多资源的一种手段或管理者。
   * 它限制了为集群平衡所做的块移动的数量及它们所占用的总宽带，是一种节流器的概念
   */
  static class BlockBalanceThrottler extends DataTransferThrottler {
   private int numThreads;// 表示当前移动数据块的线程数
   private int maxThreads;// 表示移动数据块的最大线程数
   
   /**Constructor
    * 
    * @param bandwidth Total amount of bandwidth can be used for balancing
    *
    *  集群数据块平衡节流器balanceThrottler
    */
   private BlockBalanceThrottler(long bandwidth, int maxThreads) {
     super(bandwidth);// 在父类方法中限制贷款
     // 设置移动数据块的最大线程数maxThreads
     this.maxThreads = maxThreads;
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
     LOG.info("Number threads for balancing is "+ maxThreads);
   }
   
   /** Check if the block move can start. 
    * 
    * Return true if the thread quota is not exceeded and 
    * the counter is incremented; False otherwise.
    *
    * 在移动数据块block之前，会调用acquire()方法，确认一个数据块是否可以移动。
    * 实际上是当前线程数numThreads大于等于最大线程数maxThreads时，返回false，block不可以移动；
    * 否则，当前线程数numThreads累加，并返回true，block可以移动
    * 而当数据块移动完毕后，则调用release()方法，标志移动已完成，线程计数器减一
    */
   synchronized boolean acquire() {
     // 当前线程数numThreads大于等于最大线程数maxThreads时，返回false，block不可以移动
     if (numThreads >= maxThreads) {
       return false;
     }
     // 否则，当前线程数numThreads累加，并返回true，block可以移动
     numThreads++;
     return true;
   }
   
   /**当数据块移动完毕后，则调用release()方法，标志移动已完成，线程计数器减一 */
   synchronized void release() {
     numThreads--;
   }
  }

  final BlockBalanceThrottler balanceThrottler;
  
  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. Newer clients pass the expected block size to the DataNode.
   * For older clients we just use the server-side default block size.
   *
   * 我们需要估计块大小以检测磁盘分区是否有足够的空间。
   *
   *
   */
  final long estimateBlockSize;
  
  /**
   * 在当前的构造参数中主要做DataXceiverServer中的成员变量的设置包括：
   * 1、peerServer（PeerServer是一个接口，实现了它的TcpPeerServer封装了一个ServerSocket，
   *    提供了Java Socket服务端的功能）
   * 2、设置DataNode实例datanode
   * 3、设置DataNode中DataXceiver的最大数目maxXceiverCoun
   * 4、estimateBlockSize  我们需要估计块大小以检测磁盘分区是否有足够的空间。
   * 5、balanceThrottler 设置集群平衡节流器
   * */
  DataXceiverServer(PeerServer peerServer, Configuration conf,
      DataNode datanode) {
    // 根据传入的peerServer设置同名成员变量
    this.peerServer = peerServer;

    // 设置DataNode实例datanode
    this.datanode = datanode;
    // 设置DataNode中DataXceiver的最大数目maxXceiverCount
    // 取参数dfs.datanode.max.transfer.threads，参数未配置的话，默认值为4096
    this.maxXceiverCount = 
      conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_KEY,
                  DFSConfigKeys.DFS_DATANODE_MAX_RECEIVER_THREADS_DEFAULT);
    // 设置估计块大小estimateBlockSize
    // 取参数dfs.blocksize，参数未配置的话，默认值是128*1024*1024，即128M
    this.estimateBlockSize = conf.getLongBytes(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    
    //set up parameter for cluster balancing
    // 设置集群平衡节流器
    // 带宽取参数dfs.datanode.balance.bandwidthPerSec，参数未配置默认为1024*1024
    // 最大线程数取参数dfs.datanode.balance.max.concurrent.moves，参数未配置默认为5
    //TODO spark on yarn 面试题
    this.balanceThrottler = new BlockBalanceThrottler(
        conf.getLong(DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_BANDWIDTHPERSEC_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
            DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT));
  }

  /**
   * 当datanode正常运转的时候，DataXceiverServer线程主要负责在一个while循环中利用TcpPeerServer
   * （也就是ServerSocket）的accept()方法阻塞，直到接收到客户端或者其他DataNode的连接请求，然后：
   1、获得peer，即Socket的封装；
   2、判断当前DataNode上DataXceiver线程数量是否超过阈值，如果超过的话，直接抛出IOException，
      利用IOUtils的cleanup()方法关闭peer后继续循环，否则继续3；
   3、创建一个后台线程DataXceiver，并将其加入到datanode的线程组threadGroup中，并启动该线程，响应数据读写请求；
   * */
  //TODO 核心
  @Override
  public void run() {
    Peer peer = null;
    while (datanode.shouldRun && !datanode.shutdownForUpgrade) {
      try {
        // 阻塞，直到接收到客户端或者其他DataNode的连接请求
        peer = peerServer.accept();
        // 确保DataXceiver数目没有超过最大限制
        /**
         * DataNode的getXceiverCount方法计算得到，返回线程组的活跃线程数目
         * threadGroup == null ? 0 : threadGroup.activeCount();
         */
        int curXceiverCount = datanode.getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("Xceiver count " + curXceiverCount
              + " exceeds the limit of concurrent xcievers: "
              + maxXceiverCount);
        }
        //TODO 创建一个后台线程，DataXceiver，并加入到线程组datanode.threadGroup
        new Daemon(datanode.threadGroup,
            DataXceiver.create(peer, datanode, this))
            .start();
      } catch (SocketTimeoutException ignored) {
        // wake up to see if should continue to run
        //等待唤醒看看是否能够继续运行
      } catch (AsynchronousCloseException ace) {
        // another thread closed our listener socket - that's expected during shutdown,
        // but not in other circumstances
        //异步的关闭异常
        //只有在关机的过程中，通过其他线程关闭我们的侦听套接字，其他情况下则不会发生
        if (datanode.shouldRun && !datanode.shutdownForUpgrade) {
          LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ace);
        }
      } catch (IOException ie) {
        IOUtils.cleanup(null, peer);
        LOG.warn(datanode.getDisplayName() + ":DataXceiverServer: ", ie);
      } catch (OutOfMemoryError ie) {
        IOUtils.cleanup(null, peer);
        // DataNode can run out of memory if there is too many transfers.
        // Log the event, Sleep for 30 seconds, other transfers may complete by
        // then.
        // 数据节点可能由于存在太多的数据传输导致内存溢出，记录该事件，并等待30秒，其他的数据传输可能到时就完成了
        LOG.error("DataNode is out of memory. Will retry in 30 seconds.", ie);
        try {
          Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
          // ignore
        }
      } catch (Throwable te) {
        LOG.error(datanode.getDisplayName()
            + ":DataXceiverServer: Exiting due to: ", te);
        datanode.shouldRun = false;
      }
    }

    //关闭服务器停止接收更多请求
    try {
      peerServer.close();
      closed = true;
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName()
          + " :DataXceiverServer: close exception", ie);
    }

    // 如果在重新启动前准备阶段，在关闭前通知peers
    if (datanode.shutdownForUpgrade) {
      //如果在重新启动前准备阶段，在关闭peers之前，需要先通知它们，通知的方式就是通过调用restartNotifyPeers()方法，
      // 获取peers的每个peer所在线程，通过interrupt()方法打断它们
      restartNotifyPeers();
      // Each thread needs some time to process it. If a thread needs
      // to send an OOB message to the client, but blocked on network for
      // long time, we need to force its termination.
      LOG.info("Shutting down DataXceiverServer before restart");
      // Allow roughly up to 2 seconds.
      for (int i = 0; getNumPeers() > 0 && i < 10; i++) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
    //关闭所有的peers
    closeAllPeers();
  }
  /**
   * DataXceiverServer线程被kill时，需要确定datanode的标志位shouldRun为false，或者标志位shutdownForUpgradetrue
   * 也就意味着，当datanode不应该继续运行，或者为了升级而关闭时，DataXceiverServer线程才可以被kill
   * */
  void kill() {
    assert (datanode.shouldRun == false || datanode.shutdownForUpgrade) :
      "shoudRun should be set to false or restarting should be true"
      + " before killing";
    try {
      // 关闭peerServer，即关闭ServerSocket
      this.peerServer.close();
      // 设置标志位closed为true
      this.closed = true;
    } catch (IOException ie) {
      LOG.warn(datanode.getDisplayName() + ":DataXceiverServer.kill(): ", ie);
    }
  }
  
  synchronized void addPeer(Peer peer, Thread t, DataXceiver xceiver)
      throws IOException {
    // 首先判断DataXceiverServer线程的标志位closed，为true时，说明服务线程已被关闭，不能再提供Socket通讯服务
    if (closed) {
      throw new IOException("Server closed.");
    }
    // 将peer与其所在线程t的映射关系加入到peers中
    peers.put(peer, t);
    // 将peer与其所属DataXceiver xceiver映射关系加入到peersXceiver中
    peersXceiver.put(peer, xceiver);
  }

  synchronized void closePeer(Peer peer) {
    // 从数据结构peers、peersXceiver移除peer对应记录
    peers.remove(peer);
    peersXceiver.remove(peer);
    // 利用IOUtils的cleanup关闭peer，即关闭socket
    IOUtils.cleanup(null, peer);
  }

  // Sending OOB to all peers
  public synchronized void sendOOBToPeers() {
    if (!datanode.shutdownForUpgrade) {
      return;
    }

    for (Peer p : peers.keySet()) {
      try {
        peersXceiver.get(p).sendOOB();
      } catch (IOException e) {
        LOG.warn("Got error when sending OOB message.", e);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted when sending OOB message.");
      }
    }
  }
  
  // Notify all peers of the shutdown and restart.
  // datanode.shouldRun should still be true and datanode.restarting should
  // be set true before calling this method.
  synchronized void restartNotifyPeers() {
    assert (datanode.shouldRun == true && datanode.shutdownForUpgrade);
    for (Thread t : peers.values()) {
      // interrupt each and every DataXceiver thread.
      // 中断每个DataXceiver线程
      t.interrupt();
    }
  }

  // Close all peers and clear the map.
  synchronized void closeAllPeers() {
    LOG.info("Closing all peers.");
    // 循环关闭所有的peer
    for (Peer p : peers.keySet()) {
      IOUtils.cleanup(LOG, p);
    }
    // 清空peer数据集合
    peers.clear();
    peersXceiver.clear();
  }

  // Return the number of peers.
  synchronized int getNumPeers() {
    return peers.size();
  }

  // Return the number of peers and DataXceivers.
  @VisibleForTesting
  synchronized int getNumPeersXceiver() {
    return peersXceiver.size();
  }
  
  synchronized void releasePeer(Peer peer) {
    peers.remove(peer);
    peersXceiver.remove(peer);
  }
}
