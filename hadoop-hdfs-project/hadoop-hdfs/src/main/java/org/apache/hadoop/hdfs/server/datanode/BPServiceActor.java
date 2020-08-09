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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeStatus;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.HeartbeatResponse;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * A thread per active or standby namenode to perform:
 * <ul>
 * <li> Pre-registration handshake with namenode</li>
 * <li> Registration with namenode</li>
 * <li> Send periodic heartbeats to the namenode</li>
 * <li> Handle commands received from the namenode</li>
 * </ul>
 *
 * 每个命名服务空间NameService对应着一个BPOfferService，它负责管理多个BPServiceActor工作线程，
 * 每个BPServiceActor则是DataNode上具体与每个NameNode通信完成心跳的工作线程
 *
 *
 * BPServiceActor：
 * 每个活跃active或备份standby状态NameNode对应的线程，它负责完成以下操作：
 * 1、与NameNode进行预登记握手；
 * 2、在NameNode上注册；
 * 3、发送周期性的心跳给NameNode；
 * 4、处理从NameNode接收到的请求。
 */
@InterfaceAudience.Private
//实现Runnable接口意味着BPServiceActor是一个线程
class BPServiceActor implements Runnable {
  
  static final Log LOG = DataNode.LOG;
  // NameNode地址
  final InetSocketAddress nnAddr;
  // HA服务状态
  HAServiceState state;

  // BPServiceActor线程所属BPOfferService
  final BPOfferService bpos;
  
  // lastBlockReport, lastDeletedReport and lastHeartbeat may be assigned/read
  // by testing threads (through BPServiceActor#triggerXXX), while also 
  // assigned/read by the actor thread. Thus they should be declared as volatile
  // to make sure the "happens-before" consistency.
  volatile long lastBlockReport = 0;
  volatile long lastDeletedReport = 0;

  boolean resetBlockReportTime = true;

  volatile long lastCacheReport = 0;

  Thread bpThread;
  DatanodeProtocolClientSideTranslatorPB bpNamenode;
  private volatile long lastHeartbeat = 0;
  /**
   * 枚举类，运行状态，包括：
   * CONNECTING 正在连接
   * INIT_FAILED 初始化失败
   * RUNNING 正在运行
   * EXITED 已退出
   * FAILED 已失败
   */
  static enum RunningState {
    CONNECTING, INIT_FAILED, RUNNING, EXITED, FAILED;
  }
  // 运行状态runningState默认为枚举类RunningState的CONNECTING，表示正在连接
  private volatile RunningState runningState = RunningState.CONNECTING;

  /**
   * Between block reports (which happen on the order of once an hour) the
   * DN reports smaller incremental changes to its block list. This map,
   * keyed by block ID, contains the pending changes which have yet to be
   * reported to the NN. Access should be synchronized on this object.
   */
  /**
   * 在数据块汇报（通常一小时一次）之间，DataNode会汇报其数据块列表的增量变化情况。
   * 这个Map，包含尚未汇报给NameNode的DataNode上数据块正在发生的变化。
   * 访问它必须使用synchronized关键字。
   *
   *
   * PerStoragePendingIncrementalBR就是hashMap
   * */
  private final Map<DatanodeStorage, PerStoragePendingIncrementalBR>
      pendingIncrementalBRperStorage = Maps.newHashMap();

  // IBR = Incremental Block Report. If this flag is set then an IBR will be
  // sent immediately by the actor thread without waiting for the IBR timer
  // to elapse.
  private volatile boolean sendImmediateIBR = false;
  private volatile boolean shouldServiceRun = true;
  private final DataNode dn;
  private final DNConf dnConf;

  private DatanodeRegistration bpRegistration;
  final LinkedList<BPServiceActorAction> bpThreadQueue 
      = new LinkedList<BPServiceActorAction>();
  // 构造方法，BPServiceActor被创建时就已明确知道NameNode地址InetSocketAddress类型的nnAddr，
  // 和BPOfferService类型的bpos
  BPServiceActor(InetSocketAddress nnAddr, BPOfferService bpos) {
    this.bpos = bpos;
    this.dn = bpos.getDataNode();
    this.nnAddr = nnAddr;
    this.dnConf = dn.getDnConf();
  }

  boolean isAlive() {
    if (!shouldServiceRun || !bpThread.isAlive()) {
      return false;
    }
    return runningState == BPServiceActor.RunningState.RUNNING
        || runningState == BPServiceActor.RunningState.CONNECTING;
  }

  @Override
  public String toString() {
    return bpos.toString() + " service to " + nnAddr;
  }
  
  InetSocketAddress getNNSocketAddress() {
    return nnAddr;
  }

  /**
   * Used to inject a spy NN in the unit tests.
   */
  @VisibleForTesting
  void setNameNode(DatanodeProtocolClientSideTranslatorPB dnProtocol) {
    bpNamenode = dnProtocol;
  }

  @VisibleForTesting
  DatanodeProtocolClientSideTranslatorPB getNameNodeProxy() {
    return bpNamenode;
  }

  /**
   * Perform the first part of the handshake with the NameNode.
   * This calls <code>versionRequest</code> to determine the NN's
   * namespace and version info. It automatically retries until
   * the NN responds or the DN is shutting down.
   * 
   * @return the NamespaceInfo
   *
   *
   * 通过NameNode的代理bpNamenode获取NamespaceInfo（NamespaceInfo包含了一些存储管理相关的信息，数据节点的后续处理，如版本检查、注册，都需要使用NamespaceInfo中的信息）
   * 然后校验hadoop的版本信息
   */
  @VisibleForTesting
  NamespaceInfo retrieveNamespaceInfo() throws IOException {
    NamespaceInfo nsInfo = null;
    while (shouldRun()) {
      try {
        //返回的NamespaceInfo里面有blockPoolID
        nsInfo = bpNamenode.versionRequest();
        LOG.debug(this + " received versionRequest response: " + nsInfo);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.warn("Problem connecting to server: " + nnAddr);
      } catch(IOException e ) {  // namenode is not available
        LOG.warn("Problem connecting to server: " + nnAddr);
      }
      
      // try again in a second
      sleepAndLogInterrupts(5000, "requesting version info from NN");
    }
    
    if (nsInfo != null) {
      checkNNVersion(nsInfo);
    } else {
      throw new IOException("DN shut down before block pool connected");
    }
    return nsInfo;
  }

  private void checkNNVersion(NamespaceInfo nsInfo)
      throws IncorrectVersionException {
    // build and layout versions should match
    String nnVersion = nsInfo.getSoftwareVersion();
    String minimumNameNodeVersion = dnConf.getMinimumNameNodeVersion();
    if (VersionUtil.compareVersions(nnVersion, minimumNameNodeVersion) < 0) {
      IncorrectVersionException ive = new IncorrectVersionException(
          minimumNameNodeVersion, nnVersion, "NameNode", "DataNode");
      LOG.warn(ive.getMessage());
      throw ive;
    }
    String dnVersion = VersionInfo.getVersion();
    if (!nnVersion.equals(dnVersion)) {
      LOG.info("Reported NameNode version '" + nnVersion + "' does not match " +
          "DataNode version '" + dnVersion + "' but is within acceptable " +
          "limits. Note: This is normal during a rolling upgrade.");
    }
  }

  /**
   * 1、根据namenode地址，实例化一个namenode的代理bpNamenode
   * 2、根据namenode的代理bpNamenode，获取命令空间信息
   * 3、校验命令空间是否正确（）
   *
   * */
  private void connectToNNAndHandshake() throws IOException {
    // get NN proxy
    //利用DataNode实例dn的connectToNN()方法和NameNode地址nnAddr获得NameNode的代理bpNamenode
    bpNamenode = dn.connectToNN(nnAddr);

    // First phase of the handshake with NN - get the namespace
    // info.
    // 与NameNode握手第一阶段：获取命名空间信息,并对当前的hadoop版本做了一次校验
    NamespaceInfo nsInfo = retrieveNamespaceInfo();
    
    // Verify that this matches the other NN in this HA pair.
    // This also initializes our block pool in the DN if we are
    // the first NN connection for this BP.
    // 验证，并设置命名空间信息()
    bpos.verifyAndSetNamespaceInfo(nsInfo);
    
    // Second phase of the handshake with the NN.
    // 与NameNode握手第二阶段，注册
    register(nsInfo);
  }

  // This is useful to make sure NN gets Heartbeat before Blockreport
  // upon NN restart while DN keeps retrying Otherwise,
  // 1. NN restarts.
  // 2. Heartbeat RPC will retry and succeed. NN asks DN to reregister.
  // 3. After reregistration completes, DN will send Blockreport first.
  // 4. Given NN receives Blockreport after Heartbeat, it won't mark
  //    DatanodeStorageInfo#blockContentsStale to false until the next
  //    Blockreport.
  void scheduleHeartbeat() {
    lastHeartbeat = 0;
  }

  /**
   * This methods  arranges for the data node to send the block report at 
   * the next heartbeat.
   */
  void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = monotonicNow()
      - ( dnConf.blockReportInterval - DFSUtil.getRandom().nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - dnConf.blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  
  /**
   * Report received blocks and delete hints to the Namenode for each
   * storage.
   *
   * @throws IOException
   */
  private void reportReceivedDeletedBlocks() throws IOException {

    // Generate a list of the pending reports for each storage under the lock
    //pendingIncrementalBRperStorage这个Map，包含尚未汇报给NameNode的DataNode上数据块正在发生的变化
    ArrayList<StorageReceivedDeletedBlocks> reports =new ArrayList<StorageReceivedDeletedBlocks>(pendingIncrementalBRperStorage.size());

    synchronized (pendingIncrementalBRperStorage) {
      for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry :
              pendingIncrementalBRperStorage.entrySet()) {
        final DatanodeStorage storage = entry.getKey();
        /**
         * value是里面封装了ReceivedDeletedBlockInfo
         * 这个里面封装了数据块的3种状态：
         * RECEIVING_BLOCK(1),正在接收
         * RECEIVED_BLOCK(2),已经接收
         * DELETED_BLOCK(3);已经删除
         * */
        final PerStoragePendingIncrementalBR perStorageMap = entry.getValue();
        //封装待汇报状态的block
        if (perStorageMap.getBlockInfoCount() > 0) {
          // Send newly-received and deleted blockids to namenode
          ReceivedDeletedBlockInfo[] rdbi = perStorageMap.dequeueBlockInfos();

          reports.add(new StorageReceivedDeletedBlocks(storage, rdbi));
        }
      }
      // 立即汇报的标志位sendImmediateIBR设置为false
      sendImmediateIBR = false;
    }

    if (reports.size() == 0) {
      // Nothing new to report.
      return;
    }

    // Send incremental block reports to the Namenode outside the lock
    boolean success = false;
    final long startTime = monotonicNow();
    try {
      // TODO 通过NameNode代理的blockReceivedAndDeleted()方法，将新接收的或者已删除的数据块汇报给NameNode，汇报的信息包括：
      // 1、数据节点注册信息DatanodeRegistration；
      // 2、数据块池ID；
      // 3、需要汇报的数据块及其状态信息列表StorageReceivedDeletedBlocks；
      bpNamenode.blockReceivedAndDeleted(bpRegistration,
          bpos.getBlockPoolId(),
          reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]));
      success = true;//代表是否汇报成功
    } finally {
      dn.getMetrics().addIncrementalBlockReport(monotonicNow() - startTime);
      if (!success) {// 汇报不成功的话
        synchronized (pendingIncrementalBRperStorage) {
          for (StorageReceivedDeletedBlocks report : reports) {

            // 将数据块再放回到perStorageMa
            PerStoragePendingIncrementalBR perStorageMap =
                pendingIncrementalBRperStorage.get(report.getStorage());
            perStorageMap.putMissingBlockInfos(report.getBlocks());

            // 立即汇报的标志位sendImmediateIBR设置为true
            sendImmediateIBR = true;
          }
        }
      }
    }
  }

  /**
   * @return pending incremental block report for given {@code storage}
   */
  private PerStoragePendingIncrementalBR getIncrementalBRMapForStorage(
      DatanodeStorage storage) {
    PerStoragePendingIncrementalBR mapForStorage =
        pendingIncrementalBRperStorage.get(storage);

    if (mapForStorage == null) {
      // This is the first time we are adding incremental BR state for
      // this storage so create a new map. This is required once per
      // storage, per service actor.
      mapForStorage = new PerStoragePendingIncrementalBR();
      pendingIncrementalBRperStorage.put(storage, mapForStorage);
    }

    return mapForStorage;
  }

  /**
   * Add a blockInfo for notification to NameNode. If another entry
   * exists for the same block it is removed.
   *
   * Caller must synchronize access using pendingIncrementalBRperStorage.
   */
  void addPendingReplicationBlockInfo(ReceivedDeletedBlockInfo bInfo,
      DatanodeStorage storage) {
    //pendingIncrementalBRperStorage待汇报的数据块都存储在这里，它维护了两次汇报之间收到、删除的数据块
    for (Map.Entry<DatanodeStorage, PerStoragePendingIncrementalBR> entry
            : pendingIncrementalBRperStorage.entrySet()) {
      if (entry.getValue().removeBlockInfo(bInfo)) {//移除掉传递进来，需要移除的bInfo
        break;
      }
    }
    //在把bInfo重新维护在这个map中
    getIncrementalBRMapForStorage(storage).putBlockInfo(bInfo);
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  void notifyNamenodeBlock(ReceivedDeletedBlockInfo bInfo,
      String storageUuid, boolean now) {
    synchronized (pendingIncrementalBRperStorage) {
      //更新pendingIncrementalBRperStorage
      addPendingReplicationBlockInfo(
          bInfo, dn.getFSDataset().getStorage(storageUuid));
      sendImmediateIBR = true;
      // If now is true, the report is sent right away.
      // Otherwise, it will be sent out in the next heartbeat.
      if (now) {
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }

  void notifyNamenodeDeletedBlock(
      ReceivedDeletedBlockInfo bInfo, String storageUuid) {
    synchronized (pendingIncrementalBRperStorage) {
      addPendingReplicationBlockInfo(
          bInfo, dn.getFSDataset().getStorage(storageUuid));
    }
  }

  /**
   * Run an immediate block report on this thread. Used by tests.
   */
  @VisibleForTesting
  void triggerBlockReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastBlockReport = 0;
      lastHeartbeat = 0;
      pendingIncrementalBRperStorage.notifyAll();
      while (lastBlockReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  @VisibleForTesting
  void triggerHeartbeatForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastHeartbeat = 0;
      pendingIncrementalBRperStorage.notifyAll();
      while (lastHeartbeat == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  void triggerDeletionReportForTests() {
    synchronized (pendingIncrementalBRperStorage) {
      lastDeletedReport = 0;
      pendingIncrementalBRperStorage.notifyAll();

      while (lastDeletedReport == 0) {
        try {
          pendingIncrementalBRperStorage.wait(100);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }

  @VisibleForTesting
  boolean hasPendingIBR() {
    return sendImmediateIBR;
  }

  private long prevBlockReportId = 0;

  private long generateUniqueBlockReportId() {
    long id = System.nanoTime();
    if (id <= prevBlockReportId) {
      id = prevBlockReportId + 1;
    }
    prevBlockReportId = id;
    return id;
  }

  /**
   * Report the list blocks to the Namenode
   * @return DatanodeCommands returned by the NN. May be null.
   * @throws IOException
   */
  List<DatanodeCommand> blockReport() throws IOException {
    // 1
    final long startTime = monotonicNow();
    // 如果当前时间startTime减去上次数据块汇报时间小于数据节点配置的数据块汇报时间间隔的话，直接返回null，
    // 数据节点配置的数据块汇报时间间隔取参数dfs.blockreport.intervalMsec，参数未配置的话默认为6小时
    if (startTime - lastBlockReport <= dnConf.blockReportInterval) {
      return null;
    }
    // 构造数据节点命令ArrayList列表cmds，存储数据块汇报返回的命令DatanodeCommand
    final ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

    // Flush any block information that precedes the block report. Otherwise
    // we have a chance that we will miss the delHint information
    // or we will report an RBW replica after the BlockReport already reports
    // a FINALIZED one.
    //方法发送数据块增量汇报
    reportReceivedDeletedBlocks();
    // 记录上次数据块增量汇报时间lastDeletedReport
    lastDeletedReport = startTime;
    // 设置数据块汇报起始时间brCreateStartTime为当前时间
    long brCreateStartTime = monotonicNow();
    //拿到要 汇报的数据块
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
            dn.getFSDataset().getBlockReports(bpos.getBlockPoolId());

    //2
    //数组角标
    int i = 0;
    //总汇报块数
    int totalBlockCount = 0;
    // 创建数据块汇报数组StorageBlockReport，大小为上述perVolumeBlockLists的大小
    StorageBlockReport reports[] = new StorageBlockReport[perVolumeBlockLists.size()];
    // 遍历perVolumeBlockLists
    for(Map.Entry<DatanodeStorage, BlockListAsLongs> kvPair : perVolumeBlockLists.entrySet()) {
      // 取出value：BlockListAsLong
      BlockListAsLongs blockList = kvPair.getValue();
      // 汇报的数据块保存在数组
      reports[i++] = new StorageBlockReport(kvPair.getKey(), blockList);
      // 累加要汇报的数据块儿数目
      totalBlockCount += blockList.getNumberOfBlocks();
    }

    // 3
    int numReportsSent = 0;//汇报次数
    int numRPCs = 0;//RPC请求次数
    boolean success = false;
    long brSendStartTime = monotonicNow();
    long reportId = generateUniqueBlockReportId();
    try {
      // 根据数据块总数目判断是否需要多次发送消息
      // split阈值取参数dfs.blockreport.split.threshold，参数未配置的话默认为1000*1000
      if (totalBlockCount < dnConf.blockReportSplitThreshold) {
        // 如果数据块总数目在split阈值之下，则将所有的数据块汇报信息放在一个消息中发送
        // 通过NameNode代理bpNamenode的blockReport()方法向NameNode发送数据块汇报信息
        DatanodeCommand cmd = bpNamenode.blockReport(
            bpRegistration, bpos.getBlockPoolId(), reports,
              new BlockReportContext(1, 0, reportId));
        numRPCs = 1;
        numReportsSent = reports.length;
        // 将数据块汇报后返回的命令cmd加入到命令列表cmds
        if (cmd != null) {
          cmds.add(cmd);
        }
      } else {
        for (int r = 0; r < reports.length; r++) {
          StorageBlockReport singleReport[] = { reports[r] };
          DatanodeCommand cmd = bpNamenode.blockReport(
              bpRegistration, bpos.getBlockPoolId(), singleReport,
              new BlockReportContext(reports.length, r, reportId));
          numReportsSent++;
          numRPCs++;
          if (cmd != null) {
            cmds.add(cmd);
          }
        }
      }
      success = true;
    } finally {
      // Log the block report processing stats from Datanode perspective
      // 计算数据块汇报耗时并记录在日志Log、数据节点Metrics指标体系中
      long brSendCost = monotonicNow() - brSendStartTime;
      long brCreateCost = brSendStartTime - brCreateStartTime;
      dn.getMetrics().addBlockReport(brSendCost);
      final int nCmds = cmds.size();
      LOG.info((success ? "S" : "Uns") +
          "uccessfully sent block report 0x" +
          Long.toHexString(reportId) + ",  containing " + reports.length +
          " storage report(s), of which we sent " + numReportsSent + "." +
          " The reports had " + totalBlockCount +
          " total blocks and used " + numRPCs +
          " RPC(s). This took " + brCreateCost +
          " msec to generate and " + brSendCost +
          " msecs for RPC and NN processing." +
          " Got back " +
          ((nCmds == 0) ? "no commands" :
              ((nCmds == 1) ? "one command: " + cmds.get(0) :
                  (nCmds + " commands: " + Joiner.on("; ").join(cmds)))) +
          ".");
    }
    //调度下一次数据块汇报（给下次汇报更新时间）
    scheduleNextBlockReport(startTime);
    return cmds.size() == 0 ? null : cmds;
  }

  private void scheduleNextBlockReport(long previousReportStartTime) {
    // If we have sent the first set of block reports, then wait a random
    // time before we start the periodic block reports.
    if (resetBlockReportTime) {
      lastBlockReport = previousReportStartTime -
          DFSUtil.getRandom().nextInt((int)(dnConf.blockReportInterval));
      resetBlockReportTime = false;
    } else {
      /* say the last block report was at 8:20:14. The current report
       * should have started around 9:20:14 (default 1 hour interval).
       * If current time is :
       *   1) normal like 9:20:18, next report should be at 10:20:14
       *   2) unexpected like 11:35:43, next report should be at 12:20:14
       */
      lastBlockReport += (monotonicNow() - lastBlockReport) /
          dnConf.blockReportInterval * dnConf.blockReportInterval;
    }
  }

  DatanodeCommand cacheReport() throws IOException {
    // If caching is disabled, do not send a cache report
    if (dn.getFSDataset().getCacheCapacity() == 0) {
      return null;
    }
    // send cache report if timer has expired.
    DatanodeCommand cmd = null;
    final long startTime = monotonicNow();
    if (startTime - lastCacheReport > dnConf.cacheReportInterval) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sending cacheReport from service actor: " + this);
      }
      lastCacheReport = startTime;

      String bpid = bpos.getBlockPoolId();
      List<Long> blockIds = dn.getFSDataset().getCacheReport(bpid);
      long createTime = monotonicNow();

      cmd = bpNamenode.cacheReport(bpRegistration, bpid, blockIds);
      long sendTime = monotonicNow();
      long createCost = createTime - startTime;
      long sendCost = sendTime - createTime;
      dn.getMetrics().addCacheReport(sendCost);
      LOG.debug("CacheReport of " + blockIds.size()
          + " block(s) took " + createCost + " msec to generate and "
          + sendCost + " msecs for RPC and NN processing");
    }
    return cmd;
  }
  
  HeartbeatResponse sendHeartBeat() throws IOException {

    //TODO #############################
    StorageReport[] reports =
        dn.getFSDataset().getStorageReports(bpos.getBlockPoolId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending heartbeat with " + reports.length +
                " storage reports from service actor: " + this);
    }
    
    VolumeFailureSummary volumeFailureSummary = dn.getFSDataset()
        .getVolumeFailureSummary();
    int numFailedVolumes = volumeFailureSummary != null ?
        volumeFailureSummary.getFailedStorageLocations().length : 0;
    //
    return bpNamenode.sendHeartbeat(bpRegistration,
        reports,
        dn.getFSDataset().getCacheCapacity(),
        dn.getFSDataset().getCacheUsed(),
        dn.getXmitsInProgress(),
        dn.getXceiverCount(),
        numFailedVolumes,
        volumeFailureSummary);
  }
  
  //This must be called only by BPOfferService
  void start() {
    // 保证BPServiceActor线程只启动一次
    if ((bpThread != null) && (bpThread.isAlive())) {
      //Thread is started already
      return;
    }
    bpThread = new Thread(this, formatThreadName());
    bpThread.setDaemon(true); // needed for JUnit testing
    bpThread.start();
  }
  
  private String formatThreadName() {
    Collection<StorageLocation> dataDirs =
        DataNode.getStorageLocations(dn.getConf());
    return "DataNode: [" + dataDirs.toString() + "] " +
      " heartbeating to " + nnAddr;
  }
  
  //This must be called only by blockPoolManager.
  void stop() {
    shouldServiceRun = false;
    if (bpThread != null) {
        bpThread.interrupt();
    }
  }
  
  //This must be called only by blockPoolManager
  void join() {
    try {
      if (bpThread != null) {
        bpThread.join();
      }
    } catch (InterruptedException ie) { }
  }
  
  //Cleanup method to be called by current thread before exiting.
  private synchronized void cleanUp() {
    
    shouldServiceRun = false;
    IOUtils.cleanup(LOG, bpNamenode);
    bpos.shutdownActor(this);
  }

  private void handleRollingUpgradeStatus(HeartbeatResponse resp) throws IOException {
    RollingUpgradeStatus rollingUpgradeStatus = resp.getRollingUpdateStatus();
    if (rollingUpgradeStatus != null &&
        rollingUpgradeStatus.getBlockPoolId().compareTo(bpos.getBlockPoolId()) != 0) {
      // Can this ever occur?
      LOG.error("Invalid BlockPoolId " +
          rollingUpgradeStatus.getBlockPoolId() +
          " in HeartbeatResponse. Expected " +
          bpos.getBlockPoolId());
    } else {
      bpos.signalRollingUpgrade(rollingUpgradeStatus != null);
    }
  }

  /**
   * Main loop for each BP thread. Run until shutdown,
   * forever calling remote NameNode functions.
   * 1心跳
   * 2汇报数据快
   */
  private void offerService() throws Exception {
    LOG.info("For namenode " + nnAddr + " using"
        + " DELETEREPORT_INTERVAL of " + dnConf.deleteReportInterval + " msec "
        + " BLOCKREPORT_INTERVAL of " + dnConf.blockReportInterval + "msec"
        + " CACHEREPORT_INTERVAL of " + dnConf.cacheReportInterval + "msec"
        + " Initial delay: " + dnConf.initialBlockReportDelay + "msec"
        + "; heartBeatInterval=" + dnConf.heartBeatInterval);

    //
    // Now loop for a long time....
    //
    while (shouldRun()) {
      try {
        final long startTime = monotonicNow();

        //
        // Every so often, send heartbeat or block-report
        //3秒一次 心跳
        if (startTime - lastHeartbeat >= dnConf.heartBeatInterval) {

          lastHeartbeat = startTime;
          if (!dn.areHeartbeatsDisabledForTests()) {
            /**
             * TODO 此处心跳后返回的的是指令，NameNode会将指令发送给DataNode；
             * datanode接收到指令后，在去做相应的操作（比如发现缺少副本，namenode发送同步副本指令）
             * */
            HeartbeatResponse resp = sendHeartBeat();
            assert resp != null;
            dn.getMetrics().addHeartbeat(monotonicNow() - startTime);

            // If the state of this NN has changed (eg STANDBY->ACTIVE)
            // then let the BPOfferService update itself.
            //
            // Important that this happens before processCommand below,
            // since the first heartbeat to a new active might have commands
            // that we should actually process.
            bpos.updateActorStatesFromHeartbeat(
                this, resp.getNameNodeHaState());
            state = resp.getNameNodeHaState().getState();

            if (state == HAServiceState.ACTIVE) {
              handleRollingUpgradeStatus(resp);
            }

            long startProcessCommands = monotonicNow();
            //TODO 设计模式：命令模式、指令模式
            if (!processCommand(resp.getCommands()))
              continue;
            long endProcessCommands = monotonicNow();
            if (endProcessCommands - startProcessCommands > 2000) {
              LOG.info("Took " + (endProcessCommands - startProcessCommands)
                  + "ms to process " + resp.getCommands().length
                  + " commands from NN");
            }
          }
        }
        // TODO 如果标志位sendImmediateIBR为true，或者数据块增量汇报时间已到
        if (sendImmediateIBR ||
            (startTime - lastDeletedReport > dnConf.deleteReportInterval)) {
          //发送数据块增量汇报
          reportReceivedDeletedBlocks();
          // 设置上次数据块增量汇报时间lastDeletedReport为startTime
          lastDeletedReport = startTime;
        }
        // 调用blockReport()方法，进行数据块汇报，放返回来自名字节点NameNode的相关命令cmds
        List<DatanodeCommand> cmds = blockReport();
        // 调用processCommand()方法处理来自名字节点NameNode的相关命令cmds
        processCommand(cmds == null ? null : cmds.toArray(new DatanodeCommand[cmds.size()]));

        //缓存块汇报
        DatanodeCommand cmd = cacheReport();
        processCommand(new DatanodeCommand[]{ cmd });

        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        // 计算等待时间waitTime：心跳时间间隔减去上次心跳后截至到现在已过去的时间
        long waitTime = dnConf.heartBeatInterval - (monotonicNow() - lastHeartbeat);
        synchronized(pendingIncrementalBRperStorage) {
          if (waitTime > 0 && !sendImmediateIBR) {// 如果等待时间大于0，且不是立即发送数据块增量汇报
            try {
              // 利用pendingIncrementalBRperStorage进行等待，并加synchronized关键字进行同步
              pendingIncrementalBRperStorage.wait(waitTime);
            } catch (InterruptedException ie) {
              LOG.warn("BPOfferService for " + this + " interrupted");
            }
          }
        } // synchronized
      } catch(RemoteException re) {
          String reClass = re.getClassName();
          if (UnregisteredNodeException.class.getName().equals(reClass) ||
              DisallowedDatanodeException.class.getName().equals(reClass) ||
              IncorrectVersionException.class.getName().equals(reClass)) {
            LOG.warn(this + " is shutting down", re);
            shouldServiceRun = false;
            return;
        }
        LOG.warn("RemoteException in offerService", re);
        try {
          long sleepTime = Math.min(1000, dnConf.heartBeatInterval);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      } catch (IOException e) {
        LOG.warn("IOException in offerService", e);
      }
      processQueueMessages();
    } // while (shouldRun())
  } // offerService

  /**
   * Register one bp with the corresponding NameNode
   * <p>
   * The bpDatanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID
   *  
   * issued by the namenode to recognize registered datanodes.
   * 
   * @param nsInfo current NamespaceInfo
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  void register(NamespaceInfo nsInfo) throws IOException {
    // The handshake() phase loaded the block pool storage
    // off disk - so update the bpRegistration object from that info
    bpRegistration = bpos.createRegistration();

    LOG.info(this + " beginning handshake with NN");

    while (shouldRun()) {
      try {
        // TODO
        bpRegistration = bpNamenode.registerDatanode(bpRegistration);
        bpRegistration.setNamespaceInfo(nsInfo);
        break;
      } catch(EOFException e) {  // namenode might have just restarted
        LOG.info("Problem connecting to server: " + nnAddr + " :"
            + e.getLocalizedMessage());
        sleepAndLogInterrupts(1000, "connecting to server");
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + nnAddr);
        sleepAndLogInterrupts(1000, "connecting to server");
      }
    }
    
    LOG.info("Block pool " + this + " successfully registered with NN");
    bpos.registrationSucceeded(this, bpRegistration);

    // random short delay - helps scatter the BR from all DNs
    scheduleBlockReport(dnConf.initialBlockReportDelay);
  }


  private void sleepAndLogInterrupts(int millis,
      String stateString) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.info("BPOfferService " + this + " interrupted while " + stateString);
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
   * happen either at shutdown or due to refreshNamenodes.
   *
   * 在当前线程执行中，主要做2件事：
   * 1、线程启动，在循环内1内进行Namenode的握手操作
   *    1.1：如果出现IOException异常，则将运行状态改成INIT_FAILED
   *    1.2：出现异常后会尝试重试，如果条件允许，就5s后重试，否则把运行状态切换成FAILED，退出循环
   * 2、将运行状态切换成RUNNING，然后开启循环2，不断发送心跳
   *
   *
   */
  @Override
  public void run() {
    LOG.info(this + " starting to offer service");

    try {
      // 在一个while循环内，完成连接NameNode并握手操作，即初始化
      while (true) {
        try {
          //与namonode握手，注册
          connectToNNAndHandshake();
          break;
        } catch (IOException ioe) {
          //将运行状态runningState设置为初始化失败INIT_FAILED
          // Initial handshake, storage recovery or registration failed
          runningState = RunningState.INIT_FAILED;

          // 调用shouldRetryInit()方法判断初始化失败时是否可以重试
          if (shouldRetryInit()) {
            // Retry until all namenode's of BPOS failed initialization
            LOG.error("Initialization failed for " + this + " "
                + ioe.getLocalizedMessage());
            // 线程休眠5s，并记录info日志信息，之后再进入循环重复执行之前的操作
            sleepAndLogInterrupts(5000, "initializing");
          } else {
            // 不允许重试的情况下，将运行状态runningState设置为失败FAILED，退出循环，并返回
            runningState = RunningState.FAILED;
            LOG.fatal("Initialization failed for " + this + ". Exiting. ", ioe);
            return;
          }
        }
      }
      // 设置运行状态runningState为正在运行RUNNING
      runningState = RunningState.RUNNING;
      // 进入另一个while循环，不停的调用offerService()方法，
      // 发送心跳给NameNode并接收来自NameNode命令，然后根据命令交给不同的组件去处理
      // 循环的条件就是该线程的标志位shouldServiceRun为true，且dataNode的shouldRun()返回true
      while (shouldRun()) {
        try {
          // 心跳
          offerService();
        } catch (Exception ex) {
          //// 不管抛出任何异常，都持续提供服务（包括心跳、数据块汇报等），直到datanode关闭
          LOG.error("Exception in BPOfferService for " + this, ex);
          // 存在异常的话，记录error日志，并休眠5s
          sleepAndLogInterrupts(5000, "offering service");
        }
      }
      // 设置运行状态runningState为已退出EXITED
      runningState = RunningState.EXITED;
    } catch (Throwable ex) {
      LOG.warn("Unexpected exception in block pool " + this, ex);
      runningState = RunningState.FAILED;
    } finally {
      LOG.warn("Ending block pool service for: " + this);
      // 清空，释放占用的资源
      cleanUp();
    }
  }

  private boolean shouldRetryInit() {
    return shouldRun() && bpos.shouldRetryInit();
  }

  private boolean shouldRun() {
    return shouldServiceRun && dn.shouldRun();
  }

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      for (DatanodeCommand cmd : cmds) {
        try {
          if (bpos.processCommandFromActor(cmd, this) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }


  /**
   * Report a bad block from another DN in this cluster.
   */
  void reportRemoteBadBlock(DatanodeInfo dnInfo, ExtendedBlock block)
      throws IOException {
    LocatedBlock lb = new LocatedBlock(block, 
                                    new DatanodeInfo[] {dnInfo});
    bpNamenode.reportBadBlocks(new LocatedBlock[] {lb});
  }

  void reRegister() throws IOException {
    if (shouldRun()) {
      // re-retrieve namespace info to make sure that, if the NN
      // was restarted, we still match its version (HDFS-2120)
      NamespaceInfo nsInfo = retrieveNamespaceInfo();
      // and re-register
      register(nsInfo);
      scheduleHeartbeat();
    }
  }

  private static class PerStoragePendingIncrementalBR {
    //Map<blockId , bloackInfo>
    private final Map<Long, ReceivedDeletedBlockInfo> pendingIncrementalBR =
        Maps.newHashMap();

    /**
     * Return the number of blocks on this storage that have pending
     * incremental block reports.
     * @return
     */
    int getBlockInfoCount() {
      return pendingIncrementalBR.size();
    }

    /**
     * Dequeue and return all pending incremental block report state.
     * @return
     */
    ReceivedDeletedBlockInfo[] dequeueBlockInfos() {
      ReceivedDeletedBlockInfo[] blockInfos =
          pendingIncrementalBR.values().toArray(
              new ReceivedDeletedBlockInfo[getBlockInfoCount()]);

      pendingIncrementalBR.clear();
      return blockInfos;
    }

    /**
     * Add blocks from blockArray to pendingIncrementalBR, unless the
     * block already exists in pendingIncrementalBR.
     * @param blockArray list of blocks to add.
     * @return the number of missing blocks that we added.
     */
    int putMissingBlockInfos(ReceivedDeletedBlockInfo[] blockArray) {
      int blocksPut = 0;
      for (ReceivedDeletedBlockInfo rdbi : blockArray) {
        if (!pendingIncrementalBR.containsKey(rdbi.getBlock().getBlockId())) {
          pendingIncrementalBR.put(rdbi.getBlock().getBlockId(), rdbi);
          ++blocksPut;
        }
      }
      return blocksPut;
    }

    /**
     * Add pending incremental block report for a single block.
     * @param blockInfo
     */
    void putBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      pendingIncrementalBR.put(blockInfo.getBlock().getBlockId(), blockInfo);
    }

    /**
     * Remove pending incremental block report for a single block if it
     * exists.
     *
     * @param blockInfo
     * @return true if a report was removed, false if no report existed for
     *         the given block.
     */
    boolean removeBlockInfo(ReceivedDeletedBlockInfo blockInfo) {
      return (pendingIncrementalBR.remove(blockInfo.getBlock().getBlockId()) != null);
    }
  }

  void triggerBlockReport(BlockReportOptions options) throws IOException {
    if (options.isIncremental()) {
      LOG.info(bpos.toString() + ": scheduling an incremental block report.");
      synchronized(pendingIncrementalBRperStorage) {
        sendImmediateIBR = true;
        pendingIncrementalBRperStorage.notifyAll();
      }
    } else {
      LOG.info(bpos.toString() + ": scheduling a full block report.");
      synchronized(pendingIncrementalBRperStorage) {
        lastBlockReport = 0;
        pendingIncrementalBRperStorage.notifyAll();
      }
    }
  }
  
  public void bpThreadEnqueue(BPServiceActorAction action) {
    synchronized (bpThreadQueue) {
      if (!bpThreadQueue.contains(action)) {
        bpThreadQueue.add(action);
      }
    }
  }

  private void processQueueMessages() {
    LinkedList<BPServiceActorAction> duplicateQueue;
    synchronized (bpThreadQueue) {
      duplicateQueue = new LinkedList<BPServiceActorAction>(bpThreadQueue);
      bpThreadQueue.clear();
    }
    while (!duplicateQueue.isEmpty()) {
      BPServiceActorAction actionItem = duplicateQueue.remove();
      try {
        actionItem.reportTo(bpNamenode, bpRegistration);
      } catch (BPServiceActorActionException baae) {
        LOG.warn(baae.getMessage() + nnAddr , baae);
        // Adding it back to the queue if not present
        bpThreadEnqueue(actionItem);
      }
    }
  }
}
