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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SaveNamespaceCancelledException;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Thread which runs inside the NN when it's in Standby state,
 * periodically waking up to take a checkpoint of the namespace.
 * When it takes a checkpoint, it saves it to its local
 * storage and then uploads it to the remote NameNode.tandb
 * 把自己内存里面的元数据写入磁盘，然后把磁盘上面的fsimage上传到active namenode里面 ，
 * 替换active namenode的旧fsimage文件
 */
@InterfaceAudience.Private
public class StandbyCheckpointer {
  private static final Log LOG = LogFactory.getLog(StandbyCheckpointer.class);
  private static final long PREVENT_AFTER_CANCEL_MS = 2*60*1000L;
  private final CheckpointConf checkpointConf;
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private long lastCheckpointTime;
  private final CheckpointerThread thread;
  private final ThreadFactory uploadThreadFactory;
  private URL activeNNAddress;
  private URL myNNAddress;

  private final Object cancelLock = new Object();
  private Canceler canceler;
  
  // Keep track of how many checkpoints were canceled.
  // This is for use in tests.
  private static int canceledCount = 0;
  
  public StandbyCheckpointer(Configuration conf, FSNamesystem ns)
      throws IOException {
    this.namesystem = ns;
    this.conf = conf;
    this.checkpointConf = new CheckpointConf(conf);
    //TODO
    this.thread = new CheckpointerThread();//创建CheckpointerThread线程
    this.uploadThreadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("TransferFsImageUpload-%d").build();

    setNameNodeAddresses(conf);
  }

  /**
   * Determine the address of the NN we are checkpointing
   * as well as our own HTTP address from the configuration.
   * @throws IOException 
   */
  private void setNameNodeAddresses(Configuration conf) throws IOException {
    // Look up our own address.
    myNNAddress = getHttpAddress(conf);

    // Look up the active node's address
    Configuration confForActive = HAUtil.getConfForOtherNode(conf);
    activeNNAddress = getHttpAddress(confForActive);
    
    // Sanity-check.
    Preconditions.checkArgument(checkAddress(activeNNAddress),
        "Bad address for active NN: %s", activeNNAddress);
    Preconditions.checkArgument(checkAddress(myNNAddress),
        "Bad address for standby NN: %s", myNNAddress);
  }
  
  private URL getHttpAddress(Configuration conf) throws IOException {
    final String scheme = DFSUtil.getHttpClientScheme(conf);
    String defaultHost = NameNode.getServiceAddress(conf, true).getHostName();
    URI addr = DFSUtil.getInfoServerWithDefaultHost(defaultHost, conf, scheme);
    return addr.toURL();
  }
  
  /**
   * Ensure that the given address is valid and has a port
   * specified.
   */
  private static boolean checkAddress(URL addr) {
    return addr.getPort() != 0;
  }

  public void start() {
    LOG.info("Starting standby checkpoint thread...\n" +
        "Checkpointing active NN at " + activeNNAddress + "\n" +
        "Serving checkpoints at " + myNNAddress);
    thread.start();
  }
  
  public void stop() throws IOException {
    cancelAndPreventCheckpoints("Stopping checkpointer");
    thread.setShouldRun(false);
    thread.interrupt();
    try {
      thread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    }
  }

  public void triggerRollbackCheckpoint() {
    thread.interrupt();
  }

  private void doCheckpoint() throws InterruptedException, IOException {
    assert canceler != null;
    final long txid;
    final NameNodeFile imageType;
    
    //对命名空间加cpLock锁，这是StandBy Namenode专用锁，防止checkpoint过程中发生了edit log重演
    namesystem.cpLockInterruptibly();
    try {
      //状态检查，预期情况下，StandBy NameNode所维护的editlog应该是处于open for read状态，这是StandBy NameNode启动以后的持续状态
      assert namesystem.getEditLog().isOpenForRead() :
        "Standby Checkpointer should only attempt a checkpoint when " +
        "NN is in standby mode, but the edit logs are in an unexpected state";
      
      FSImage img = namesystem.getFSImage();
      //上一次checkpoint完成的位置
      long prevCheckpointTxId = img.getStorage().getMostRecentCheckpointTxId();
      //当前最后一次操作的位置，即本次checkpoint需要到达的位置
      long thisCheckpointTxId = img.getLastAppliedOrWrittenTxId();

      //基于位置做校验
      assert thisCheckpointTxId >= prevCheckpointTxId;
      if (thisCheckpointTxId == prevCheckpointTxId) {
        LOG.info("A checkpoint was triggered but the Standby Node has not " +
            "received any transactions since the last checkpoint at txid " +
            thisCheckpointTxId + ". Skipping...");
        return;
      }

      if (namesystem.isRollingUpgrade()
          && !namesystem.getFSImage().hasRollbackFSImage()) {
        // if we will do rolling upgrade but have not created the rollback image
        // yet, name this checkpoint as fsimage_rollback
        imageType = NameNodeFile.IMAGE_ROLLBACK;
      } else {
        imageType = NameNodeFile.IMAGE;
      }
      //TODO 进行checkpoint操作
      img.saveNamespace(namesystem, imageType, canceler);
      txid = img.getStorage().getMostRecentCheckpointTxId();
      assert txid == thisCheckpointTxId : "expected to save checkpoint at txid=" +
        thisCheckpointTxId + " but instead saved at txid=" + txid;

      // Save the legacy OIV image, if the output dir is defined.
      String outputDir = checkpointConf.getLegacyOivImageDir();
      if (outputDir != null && !outputDir.isEmpty()) {
        img.saveLegacyOIVImage(namesystem, outputDir, canceler);
      }
    } finally {
      namesystem.cpUnlock();
    }

    //TODO  采用异步方式，将当前的img文件发送到远程的Active NameNode
    /**
     * 通过ExecutorService创建了一个独立线程负责文件传输过程。
     * 这是因为image文件一般较大，传输较为耗时，因此会创建一个单独线程执行
     * */
    ExecutorService executor =
        Executors.newSingleThreadExecutor(uploadThreadFactory);
    Future<Void> upload = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        TransferFsImage.uploadImageFromStorage(activeNNAddress, conf,
            namesystem.getFSImage().getStorage(), imageType, txid, canceler);
        return null;
      }
    });
    executor.shutdown();
    try {
      upload.get();
    } catch (InterruptedException e) {
      // The background thread may be blocked waiting in the throttler, so
      // interrupt it.
      upload.cancel(true);
      throw e;
    } catch (ExecutionException e) {
      throw new IOException("Exception during image upload: " + e.getMessage(),
          e.getCause());
    }
  }
  
  /**
   * Cancel any checkpoint that's currently being made,
   * and prevent any new checkpoints from starting for the next
   * minute or so.
   */
  public void cancelAndPreventCheckpoints(String msg) throws ServiceFailedException {
    synchronized (cancelLock) {
      // The checkpointer thread takes this lock and checks if checkpointing is
      // postponed. 
      thread.preventCheckpointsFor(PREVENT_AFTER_CANCEL_MS);

      // Before beginning a checkpoint, the checkpointer thread
      // takes this lock, and creates a canceler object.
      // If the canceler is non-null, then a checkpoint is in
      // progress and we need to cancel it. If it's null, then
      // the operation has not started, meaning that the above
      // time-based prevention will take effect.
      if (canceler != null) {
        canceler.cancel(msg);
      }
    }
  }
  
  @VisibleForTesting
  static int getCanceledCount() {
    return canceledCount;
  }

  private long countUncheckpointedTxns() {
    FSImage img = namesystem.getFSImage();
    //在FSImage中的saveFSImage 保存了上一次chekpoint的事务ID
    return img.getLastAppliedOrWrittenTxId() -
      img.getStorage().getMostRecentCheckpointTxId();
  }

  /**
   * 这个线程的任务，就是通过判断距离上一次checkpoint操作的时间是否超过阈值
   * (dfs.namenode.checkpoint.period，默认3600s，即1个小时)，
   * 以及当前没有进行checkpoint操作的数据量是否超过阈值(dfs.namenode.checkpoint.txns，默认1000000)
   * 来判断是否应该进行checkpoint操作。
   *
   * 在完成了checkpoint操作，生成了对应的img文件以后，会通过HTTP PUT操作，将这个文件发送到Active NameNode
   * */
  private class CheckpointerThread extends Thread {
    private volatile boolean shouldRun = true;
    private volatile long preventCheckpointsUntil = 0;

    private CheckpointerThread() {
      super("Standby State Checkpointer");
    }
    
    private void setShouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }

    @Override
    public void run() {
      // We have to make sure we're logged in as far as JAAS
      // is concerned, in order to use kerberized SSL properly.
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
     * Prevent checkpoints from occurring for some time period
     * in the future. This is used when preparing to enter active
     * mode. We need to not only cancel any concurrent checkpoint,
     * but also prevent any checkpoints from racing to start just
     * after the cancel call.
     * 
     * @param delayMs the number of MS for which checkpoints will be
     * prevented
     */
    private void preventCheckpointsFor(long delayMs) {
      preventCheckpointsUntil = monotonicNow() + delayMs;
    }

    private void doWork() {
      //定义checkpoint的时间间隔 1分钟
      final long checkPeriod = 1000 * checkpointConf.getCheckPeriod();
      //最新一次checkpoint的时间
      lastCheckpointTime = monotonicNow();
      while (shouldRun) {
        //是否需要回滚checkpoint
        boolean needRollbackCheckpoint = namesystem.isNeedRollbackFsImage();
        if (!needRollbackCheckpoint) {
          try {
            //每隔60秒，检查是否需要checkpoint
            Thread.sleep(checkPeriod);
          } catch (InterruptedException ie) {
          }
          if (!shouldRun) {
            break;
          }
        }
        try {
          // We may have lost our ticket since last checkpoint, log in again, just in case
          if (UserGroupInformation.isSecurityEnabled()) {
            UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
          }
          
          final long now = monotonicNow();
          //TODO checkpoint的条件1: 现在有多少条日志没有checkpoint
          final long uncheckpointed = countUncheckpointedTxns();
          //TODO checkpoint的条件2：已经有多久没有checkpoint了
          final long secsSinceLast = (now - lastCheckpointTime) / 1000;//在FSImage中的saveFSImage 保存了上一次chekpoint的时间
          
          boolean needCheckpoint = needRollbackCheckpoint;//默认false
          if (needCheckpoint) {
            LOG.info("Triggering a rollback fsimage for rolling upgrade.");
            //TODO 条件1：如果超过了100万条日志没有做checkpoint ， 则做一次checkpoint
          } else if (uncheckpointed >= checkpointConf.getTxnCount()) {
            LOG.info("Triggering checkpoint because there have been " + 
                uncheckpointed + " txns since the last checkpoint, which " +
                "exceeds the configured threshold " +
                checkpointConf.getTxnCount());
            needCheckpoint = true;
            //TODO 条件2：如果超过了1小时没有做checkpoint ， 则做一次checkpoint
          } else if (secsSinceLast >= checkpointConf.getPeriod()) {
            LOG.info("Triggering checkpoint because it has been " +
                secsSinceLast + " seconds since the last checkpoint, which " +
                "exceeds the configured interval " + checkpointConf.getPeriod());
            needCheckpoint = true;
          }
          //如果调用了checkpoint的stop操作 ， 则此会做取消checkpoint的操作
          synchronized (cancelLock) {
            if (now < preventCheckpointsUntil) {
              LOG.info("But skipping this checkpoint since we are about to failover!");
              canceledCount++;
              continue;
            }
            assert canceler == null;
            canceler = new Canceler();
          }
          //满足条件，需要做checkpoint
          if (needCheckpoint) {
            //TODO
            doCheckpoint();
            // reset needRollbackCheckpoint to false only when we finish a ckpt
            // for rollback image
            if (needRollbackCheckpoint
                && namesystem.getFSImage().hasRollbackFSImage()) {
              namesystem.setCreatedRollbackImages(true);
              namesystem.setNeedRollbackFsImage(false);
            }
            lastCheckpointTime = now;
          }
        } catch (SaveNamespaceCancelledException ce) {
          LOG.info("Checkpoint was cancelled: " + ce.getMessage());
          canceledCount++;
        } catch (InterruptedException ie) {
          LOG.info("Interrupted during checkpointing", ie);
          // Probably requested shutdown.
          continue;
        } catch (Throwable t) {
          LOG.error("Exception in doCheckpoint", t);
        } finally {
          synchronized (cancelLock) {
            canceler = null;
          }
        }
      }
    }
  }

  @VisibleForTesting
  URL getActiveNNAddress() {
    return activeNNAddress;
  }
}
