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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.util.ExitUtil.terminate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 *  editLogTailer是一个编辑日志edit log的追踪器，
 *  它的主要作用就是当NameNode处于standby状态时用于从共享的edit log读取数据。
 *  它的构造是在FSNamesystem的startStandbyServices()方法中
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Log LOG = LogFactory.getLog(EditLogTailer.class);

  // 编辑日志跟踪线程EditLogTailerThread实例tailerThread
  private final EditLogTailerThread tailerThread;
  // HDFS配置信息Configuration实例conf
  private final Configuration conf;
  // 文件系统命名空间
  private final FSNamesystem namesystem;
  // 文件系统编辑日志FSEditLog
  private FSEditLog editLog;
  // 文件系统编辑日志FSEditLog
  private InetSocketAddress activeAddr;
  // 名字节点通信接口
  private NamenodeProtocol cachedActiveProxy = null;

  /**
   * 一次编辑日志滚动开始时的最新事务ID
   */
  private long lastRollTriggerTxId = HdfsConstants.INVALID_TXID;
  
  /**
   * StandBy NameNode加载的最高事务ID
   */
  private long lastLoadedTxnId = HdfsConstants.INVALID_TXID;

  /**
   * The last time we successfully loaded a non-zero number of edits from the
   * shared directory.
   * 最后一次我们从共享目录成功加载一个非零编辑的时间
   */
  private long lastLoadTimeMs;

  /**
   * How often the Standby should roll edit logs. Since the Standby only reads
   * from finalized log segments, the Standby will only be as up-to-date as how
   * often the logs are rolled.
   * StandBy NameNode滚动编辑日志的时间间隔。2min
   */
  private final long logRollPeriodMs;

  /**
   * How often the Standby should check if there are new finalized segment(s)
   * available to be read from.
   * StandBy NameNode检查是否存在可以读取的新的最终日志段的时间间隔
   */
  private final long sleepTimeMs;//1分钟

  //TODO
  public EditLogTailer(FSNamesystem namesystem, Configuration conf) {
    // TODO 实例化编辑日志追踪线程EditLogTailerThread
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;
    this.namesystem = namesystem;
      // 从namesystem中获取editLog
    this.editLog = namesystem.getEditLog();
    // 最新加载edit log时间lastLoadTimestamp初始化为当前时间
    lastLoadTimeMs = monotonicNow();
    // StandBy NameNode滚动编辑日志的时间间隔logRollPeriodMs
    // 取参数dfs.ha.log-roll.period，参数未配置默认为2min
    logRollPeriodMs = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT) * 1000;
    //如果StandBy NameNode滚动编辑日志的时间间隔logRollPeriodMs > 0
    if (logRollPeriodMs >= 0) {
      // 调用getActiveNodeAddress()方法初始化Active NameNode地址activeAddr
      this.activeAddr = getActiveNodeAddress();
      Preconditions.checkArgument(activeAddr.getPort() > 0,
          "Active NameNode must have an IPC port configured. " +
          "Got address '%s'", activeAddr);
      LOG.info("Will roll logs on active node at " + activeAddr + " every " +
          (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
          DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }
    // StandBy NameNode检查是否存在可以读取的新的最终日志段的时间间隔sleepTimeMs
    // 取参数dfs.ha.tail-edits.period，参数未配置默认为1min
    sleepTimeMs = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT) * 1000;
    
    LOG.debug("logRollPeriodMs=" + logRollPeriodMs +
        " sleepTime=" + sleepTimeMs);
  }
  
  private InetSocketAddress getActiveNodeAddress() {
    Configuration activeConf = HAUtil.getConfForOtherNode(conf);
    return NameNode.getServiceAddress(activeConf, true);
  }
  
  private NamenodeProtocol getActiveNodeProxy() throws IOException {
    if (cachedActiveProxy == null) {
      int rpcTimeout = conf.getInt(
          DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_KEY,
          DFSConfigKeys.DFS_HA_LOGROLL_RPC_TIMEOUT_DEFAULT);
      NamenodeProtocolPB proxy = RPC.waitForProxy(NamenodeProtocolPB.class,
          RPC.getProtocolVersion(NamenodeProtocolPB.class), activeAddr, conf,
          rpcTimeout, Long.MAX_VALUE);
      cachedActiveProxy = new NamenodeProtocolTranslatorPB(proxy);
    }
    assert cachedActiveProxy != null;
    return cachedActiveProxy;
  }

  public void start() {
    tailerThread.start();
  }
  
  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  public void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }
  
  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    // Important to do tailing as the login user, in case the shared
    // edits storage is implemented by a JournalManager that depends
    // on security credentials to access the logs (eg QuorumJournalManager).
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try {
          // It is already under the full name system lock and the checkpointer
          // thread is already stopped. No need to acqure any other lock.
          doTailEdits();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
        return null;
      }
    });
  }

  /**
   * 整段方法的概括：
   * 首先整段方法是处于namesystem的锁下进行的，主要做以下内容
   * 1、根据加载到的image来获取lastTxnId
   * 2、editLog会根据这个事务ID+1，来获取编辑日志输入流集合streams
   * 3、有了streams流之后，根据这个流来加载edits文件
   * 4、如果加载到了文件，那么更新加载时间和lastLoadedTxnId
   *
   * */
  @VisibleForTesting
  void doTailEdits() throws IOException, InterruptedException {
    //加锁
    namesystem.writeLockInterruptibly();
    try {
      // 加载当前自己的元数据日志
      FSImage image = namesystem.getFSImage();
      // 通过文件系统镜像FSImage实例image获取最新的事务ID
      long lastTxnId = image.getLastAppliedTxId();
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("lastTxnId: " + lastTxnId);
      }
      Collection<EditLogInputStream> streams;
      try {
        //TODO
        // 从编辑日志editLog中获取编辑日志输入流集合streams，获取的输入流为最新事务ID加1之后的数据（//false代表不允许获取处于in-progress状态的文件）
        //得到journalNode上edit log的输入流
        streams = editLog.selectInputStreams(lastTxnId + 1, 0, null, false);
      } catch (IOException ioe) {
        LOG.warn("Edits tailer failed to find any streams. Will try again " +
            "later.", ioe);
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("edit streams to load from: " + streams.size());
      }
      long editsLoaded = 0;
      try {
        // 调用文件系统镜像FSImage实例image的loadEdits()，
        // 利用编辑日志输入流集合streams，加载编辑日志至目标namesystem中的文件系统镜像FSImage，
        // 并获得编辑日志加载的大小editsLoaded
        //TODO 去journalNode上加载日志
        editsLoaded = image.loadEdits(streams, namesystem);
      } catch (EditLogInputException elie) {
        editsLoaded = elie.getNumEditsLoaded();
        throw elie;
      } finally {
        if (editsLoaded > 0 || LOG.isDebugEnabled()) {
          LOG.info(String.format("Loaded %d edits starting from txid %d ",
              editsLoaded, lastTxnId));
        }
      }
      //如果加载到数据
      if (editsLoaded > 0) {
        //更新最后一次我们从共享目录成功加载一个非零编辑的时间
        lastLoadTimeMs = monotonicNow();
      }
      //记录同步过来的最后一个transactionId，代表当前Standby NameNode的同步位置
      lastLoadedTxnId = image.getLastAppliedTxId();
    } finally {
      // namesystem去除写锁
      namesystem.writeUnlock();
    }
  }

  /**
   * @return time in msec of when we last loaded a non-zero number of edits.
   */
  public long getLastLoadTimeMs() {
    return lastLoadTimeMs;
  }

  /**
   * @return true if the configured log roll period has elapsed.
   */
  //到达StandBy NameNode滚动编辑日志的时间间隔
  private boolean tooLongSinceLastLoad() {
    return logRollPeriodMs >= 0 && 
      (monotonicNow() - lastLoadTimeMs) > logRollPeriodMs ;
  }

  /**
   * Trigger the active node to roll its logs.
   */
  private void triggerActiveLogRoll() {
    LOG.info("Triggering log roll on remote NameNode " + activeAddr);
    try {
      // 获得Active NameNode的代理，并调用其rollEditLog()方法滚动编辑日志
      getActiveNodeProxy().rollEditLog();
      // 将上次StandBy NameNode加载的最高事务ID，即lastLoadedTxnId，
      // 赋值给上次编辑日志滚动开始时的最新事务ID，即lastRollTriggerTxId，
      // 这么做是为了方便进行日志回滚
      lastRollTriggerTxId = lastLoadedTxnId;
    } catch (IOException ioe) {
      LOG.warn("Unable to trigger a roll of the active NN", ioe);
    }
  }

  /**
   * The thread which does the actual work of tailing edits journals and
   * applying the transactions to the FSNS.
   */
  private class EditLogTailerThread extends Thread {
    private volatile boolean shouldRun = true;
    
    private EditLogTailerThread() {
      super("Edit log tailer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    @Override
    public void run() {
      SecurityUtil.doAsLoginUserOrFatal(
          new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            doWork();
            return null;
          }
        });
    }

    /**
     * Standby NameNode 使用EditLogTailer线程来负责向远程的QuorumJournalNode读取新的EditLog文件。
     * 在Standby NameNode启动的时候，构造了EditLogTailer对象，
     * 并启动了线程EditLogTailerThread来不断和远程的QuorumJournalNode通信以拉取新的log segment文件。
     * */
    private void doWork() {
      while (shouldRun) {
        try {
          // 达到StandBy NameNode滚动编辑日志的时间间隔（2min） &&
          // 上一次编辑日志滚动开始时的最新事务ID < StandBy NameNode加载的最高事务ID
          if (tooLongSinceLastLoad() && lastRollTriggerTxId < lastLoadedTxnId) {
            //TODO
            //调用一次triggerActiveLogRoll用来告知远程的Active NameNode进行一次roll操作，
            // NameNode roll操作会告知QJM也进行roll操作，这样，Standby Namenode就可以拉取到这个segment
            triggerActiveLogRoll();
          }
          /**
           * Check again in case someone calls {@link EditLogTailer#stop} while
           * we're triggering an edit log roll, since ipc.Client catches and
           * ignores {@link InterruptedException} in a few places. This fixes
           * the bug described in HDFS-2823.
           */
          // 判断标志位shouldRun，如果其为false的话，退出循环
          if (!shouldRun) {
            break;
          }
          // Prevent reading of name system while being modified. The full
          // name system lock will be acquired to further block even the block
          // state updates.
          namesystem.cpLockInterruptibly();
          try {
            //TODO
            //standByNameNode开始拉取远程的segment文件
            doTailEdits();
          } finally {
            namesystem.cpUnlock();
          }
        } catch (EditLogInputException elie) {
          LOG.warn("Error while reading edits from disk. Will try again.", elie);
        } catch (InterruptedException ie) {
          // interrupter should have already set shouldRun to false
          continue;
        } catch (Throwable t) {
          LOG.fatal("Unknown error encountered while tailing edits. " +
              "Shutting down standby NN.", t);
          terminate(1, t);
        }

        try {
          //每隔1分钟 检查是否存在可以读取的新的最终日志段的时间间隔
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOG.warn("Edit log tailer interrupted", e);
        }
      }
    }
  }

}
