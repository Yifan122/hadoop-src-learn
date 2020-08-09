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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.io.nativeio.NativeIO;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 * 
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
@InterfaceAudience.Private
public class FileJournalManager implements JournalManager {
  private static final Log LOG = LogFactory.getLog(FileJournalManager.class);

  private final Configuration conf;
  private final StorageDirectory sd;
  private final StorageErrorReporter errorReporter;
  //TODO
//  private int outputBufferCapacity = 512*1024;
  private int outputBufferCapacity ;

  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");
  private static final Pattern EDITS_INPROGRESS_STALE_REGEX = Pattern.compile(
      NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+).*(\\S+)");

  private File currentInProgress = null;

  @VisibleForTesting
  StoragePurger purger
    = new NNStorageRetentionManager.DeletionStoragePurger();

  public FileJournalManager(Configuration conf, StorageDirectory sd,
      StorageErrorReporter errorReporter) {
    this.conf = conf;
    this.sd = sd;
    this.errorReporter = errorReporter;
    this.outputBufferCapacity = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_BUFFER_VALUE_KEY ,
            DFSConfigKeys.DFS_NAMENODE_BUFFER_DEFAULT_VALUE);

  }

  @Override 
  public void close() throws IOException {}
  
  @Override
  public void format(NamespaceInfo ns) throws IOException {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean hasSomeData() {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }

  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid,
      int layoutVersion) throws IOException {
    try {
      //获取处于inProcess状态的Edit文件
      currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
      //todo
      EditLogOutputStream stm = new EditLogFileOutputStream(conf,
          currentInProgress, outputBufferCapacity);
      stm.create(layoutVersion);
      return stm;
    } catch (IOException e) {
      LOG.warn("Unable to start log segment " + txid +
          " at " + currentInProgress + ": " +
          e.getLocalizedMessage());
      errorReporter.reportErrorOnFile(currentInProgress);
      throw e;
    }
  }

  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    //根据起始的事务ID获取in-process状态的edit文件
    File inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);

    //根据起始事务id和结束事务ID，创建一个类似：edits_00000000001_000000000008 的edit空文件
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");

    try {
      //然后把in-process文件的内容拷贝到新创建的文件里
      NativeIO.renameTo(inprogressFile, dstFile);
    } catch (IOException e) {
      errorReporter.reportErrorOnFile(dstFile);
      throw new IllegalStateException("Unable to finalize edits file " + inprogressFile, e);
    }
    //最后把in-process的文件赋null
    if (inprogressFile.equals(currentInProgress)) {
      currentInProgress = null;
    }
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  synchronized public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    LOG.info("Purging logs older than " + minTxIdToKeep);
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    List<EditLogFile> editLogs = matchEditLogs(files, true);
    for (EditLogFile log : editLogs) {
      if (log.getFirstTxId() < minTxIdToKeep &&
          log.getLastTxId() < minTxIdToKeep) {
        purger.purgeLog(log);
      }
    }
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * @param firstTxId the txnid which to start looking
   * @param inProgressOk whether or not to include the in-progress edit log 
   *        segment       
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  public List<RemoteEditLog> getRemoteEditLogs(long firstTxId,
      boolean inProgressOk) throws IOException {
    //获取journalNode存储的edit文件目录
    File currentDir = sd.getCurrentDir();
    //通过正则表达式，拿到editlog的文件列表清单信息
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);

    //迭代列表清单集合allLogFiles ， 根据条件（除了in-process状态的）然后封装到List<RemoteEditLog> ret 并返回
    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(
        allLogFiles.size());
    for (EditLogFile elf : allLogFiles) {
      if (elf.hasCorruptHeader() || (!inProgressOk && elf.isInProgress())) {
        continue;
      }
      if (elf.isInProgress()) {
        try {
          elf.validateLog();
        } catch (IOException e) {
          LOG.error("got IOException while trying to validate header of " +
              elf + ".  Skipping.", e);
          continue;
        }
      }
      if (elf.getFirstTxId() >= firstTxId) {
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      } else if (elf.getFirstTxId() < firstTxId && firstTxId <= elf.getLastTxId()) {
        // If the firstTxId is in the middle of an edit log segment. Return this
        // anyway and let the caller figure out whether it wants to use it.
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      }
    }
    
    Collections.sort(ret);
    
    return ret;
  }
  
  /**
   * Discard all editlog segments whose first txid is greater than or equal to
   * the given txid, by renaming them with suffix ".trash".
   */
  private void discardEditLogSegments(long startTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<EditLogFile> toTrash = Lists.newArrayList();
    LOG.info("Discard the EditLog files, the given start txid is " + startTxId);
    // go through the editlog files to make sure the startTxId is right at the
    // segment boundary
    for (EditLogFile elf : allLogFiles) {
      if (elf.getFirstTxId() >= startTxId) {
        toTrash.add(elf);
      } else {
        Preconditions.checkState(elf.getLastTxId() < startTxId);
      }
    }

    for (EditLogFile elf : toTrash) {
      // rename these editlog file as .trash
      elf.moveAsideTrashFile(startTxId);
      LOG.info("Trash the EditLog file " + elf);
    }
  }

  /**
   * returns matching edit logs via the log directory. Simple helper function
   * that lists the files in the logDir and calls matchEditLogs(File[])
   * 
   * @param logDir
   *          directory to match edit logs in
   * @return matched edit logs
   * @throws IOException
   *           IOException thrown for invalid logDir
   */
  public static List<EditLogFile> matchEditLogs(File logDir) throws IOException {
    return matchEditLogs(FileUtil.listFiles(logDir));
  }
  
  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    return matchEditLogs(filesInStorage, false);
  }

  private static List<EditLogFile> matchEditLogs(File[] filesInStorage,
      boolean forPurging) {
    // 创建编辑日志文件EditLogFile列表ret
    List<EditLogFile> ret = Lists.newArrayList();
    // 遍历filesInStorage（current目录），对每个文件进行处理
    for (File f : filesInStorage) {
      // 获得文件名
      String name = f.getName();
      //// Check for edits
      // 根据文件名，利用正则表达式，检测其是否为编辑日志edits log
      // 正则表达式为edits_(\d+)-(\d+)
      // 原edits_(\\d+)-(\\d+)中d前面的两个\中第一个只是转义符
      // 文件名类似如下：edits_0000000000001833048-0000000000001833081
      // 第一串数字为起始事务ID，第二串数字为终止事务ID
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      // 文件名匹配正则表达式的话，说明其是我们需要寻找的编辑日志文件
      if (editsMatch.matches()) {
        try {
          // 获取起始事务ID：正则表达式中第一个匹配的是起始事务ID
          long startTxId = Long.parseLong(editsMatch.group(1));
          // 获取终止事务ID：正则表达式中第二个匹配的是终止事务ID
          long endTxId = Long.parseLong(editsMatch.group(2));
          // 利用文件f、起始事务startTxId、终止事务endTxId构造编辑日志文件EditLogFile实例，
          // 并添加到ret列表中
          ret.add(new EditLogFile(f, startTxId, endTxId));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }
      }

      // 检测正在处理中的编辑日志
      // 正则表达式为edits_inprogress_(\d+)
      // 原edits_inprogress_(\\d+)中d前面的两个\中第一个只是转义符
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.parseLong(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(f, startTxId, HdfsConstants.INVALID_TXID, true));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("In-progress edits file " + f + " has improperly " +
                    "formatted transaction ID");
          // skip
        }
      }
      if (forPurging) {
        // Check for in-progress stale edits
        Matcher staleInprogressEditsMatch = EDITS_INPROGRESS_STALE_REGEX
            .matcher(name);
        if (staleInprogressEditsMatch.matches()) {
          try {
            long startTxId = Long.parseLong(staleInprogressEditsMatch.group(1));
            ret.add(new EditLogFile(f, startTxId, HdfsConstants.INVALID_TXID,
                true));
            continue;
          } catch (NumberFormatException nfe) {
            LOG.error("In-progress stale edits file " + f + " has improperly "
                + "formatted transaction ID");
            // skip
    }
        }
      }
    }
    return ret;
  }

  @Override
  synchronized public void selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxId,
      boolean inProgressOk) throws IOException {
    // 先调用StorageDirectory的getCurrentDir()方法获得其current目录，
    // 然后再调用matchEditLogs()方法，获得编辑日志文件EditLogFile列表elfs
    List<EditLogFile> elfs = matchEditLogs(sd.getCurrentDir());

    LOG.debug(this + ": selecting input streams starting at " + fromTxId + 
        (inProgressOk ? " (inProgress ok) " : " (excluding inProgress) ") +
        "from among " + elfs.size() + " candidate file(s)");
    // 调用addStreamsToCollectionFromFiles()方法，根据编辑日志文件列表elfs获得输入流，并添加到输入流列表streams
    addStreamsToCollectionFromFiles(elfs, streams, fromTxId, inProgressOk);
  }

  //elfs是current目录集合
  static void addStreamsToCollectionFromFiles(Collection<EditLogFile> elfs,
      Collection<EditLogInputStream> streams, long fromTxId, boolean inProgressOk) {
    // 遍历EditLogFile集合elfs，针对每个EditLogFile实例elf进行如下处理：
    for (EditLogFile elf : elfs) {
      // 如果elf处于处理过程中
      if (elf.isInProgress()) {
        // 如果不需要获取处于处理过程中的编辑日志，直接跳过
        if (!inProgressOk) {
          LOG.debug("passing over " + elf + " because it is in progress " +
              "and we are ignoring in-progress logs.");
          continue;
        }
        try {
          // 校验编辑日志，校验不成功的话，直接跳过
          elf.validateLog();
        } catch (IOException e) {
          LOG.error("got IOException while trying to validate header of " +
              elf + ".  Skipping.", e);
          continue;
        }
      }
      // 如果elf的最后事务lastTxId小于我们获取编辑日志的起始事务艾迪fromTxId，直接跳过
      if (elf.lastTxId < fromTxId) {
        assert elf.lastTxId != HdfsConstants.INVALID_TXID;
        LOG.debug("passing over " + elf + " because it ends at " +
            elf.lastTxId + ", but we only care about transactions " +
            "as new as " + fromTxId);
        continue;
      }
      // 利用elf中的文件file、起始事务firstTxId、终止事务lastTxId、编辑日志文件是否处于处理过程中标志位isInProgress，
      // 构造编辑日志文件输入流EditLogFileInputStream实例elfis
      EditLogFileInputStream elfis = new EditLogFileInputStream(elf.getFile(),
            elf.getFirstTxId(), elf.getLastTxId(), elf.isInProgress());
      LOG.debug("selecting edit log stream " + elf);
      // 将elfis加入到输入流列表stream
      streams.add(elfis);
    }
  }

  @Override
  synchronized public void recoverUnfinalizedSegments() throws IOException {
    File currentDir = sd.getCurrentDir();
    LOG.info("Recovering unfinalized segments in " + currentDir);
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);

    for (EditLogFile elf : allLogFiles) {
      if (elf.getFile().equals(currentInProgress)) {
        continue;
      }
      if (elf.isInProgress()) {
        // If the file is zero-length, we likely just crashed after opening the
        // file, but before writing anything to it. Safe to delete it.
        if (elf.getFile().length() == 0) {
          LOG.info("Deleting zero-length edit log file " + elf);
          if (!elf.getFile().delete()) {
            throw new IOException("Unable to delete file " + elf.getFile());
          }
          continue;
        }

        elf.validateLog();

        if (elf.hasCorruptHeader()) {
          elf.moveAsideCorruptFile();
          throw new CorruptionException("In-progress edit log file is corrupt: "
              + elf);
        }
        if (elf.getLastTxId() == HdfsConstants.INVALID_TXID) {
          // If the file has a valid header (isn't corrupt) but contains no
          // transactions, we likely just crashed after opening the file and
          // writing the header, but before syncing any transactions. Safe to
          // delete the file.
          LOG.info("Moving aside edit log file that seems to have zero " +
              "transactions " + elf);
          elf.moveAsideEmptyFile();
          continue;
        }
        finalizeLogSegment(elf.getFirstTxId(), elf.getLastTxId());
      }
    }
  }

  public List<EditLogFile> getLogFiles(long fromTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<EditLogFile> logFiles = Lists.newArrayList();
    
    for (EditLogFile elf : allLogFiles) {
      if (fromTxId <= elf.getFirstTxId() ||
          elf.containsTxId(fromTxId)) {
        logFiles.add(elf);
      }
    }
    
    Collections.sort(logFiles, EditLogFile.COMPARE_BY_START_TXID);

    return logFiles;
  }
  
  public EditLogFile getLogFile(long startTxId) throws IOException {
    return getLogFile(sd.getCurrentDir(), startTxId);
  }
  
  public static EditLogFile getLogFile(File dir, long startTxId)
      throws IOException {
    List<EditLogFile> files = matchEditLogs(dir);
    List<EditLogFile> ret = Lists.newLinkedList();
    for (EditLogFile elf : files) {
      if (elf.getFirstTxId() == startTxId) {
        ret.add(elf);
      }
    }
    
    if (ret.isEmpty()) {
      // no matches
      return null;
    } else if (ret.size() == 1) {
      return ret.get(0);
    } else {
      throw new IllegalStateException("More than one log segment in " + 
          dir + " starting at txid " + startTxId + ": " +
          Joiner.on(", ").join(ret));
    }
  }

  @Override
  public String toString() {
    return String.format("FileJournalManager(root=%s)", sd.getRoot());
  }

  /**
   * Record of an edit log that has been located and had its filename parsed.
   */
  @InterfaceAudience.Private
  public static class EditLogFile {
    private File file;
    private final long firstTxId;
    private long lastTxId;

    private boolean hasCorruptHeader = false;
    private final boolean isInProgress;

    final static Comparator<EditLogFile> COMPARE_BY_START_TXID 
      = new Comparator<EditLogFile>() {
      @Override
      public int compare(EditLogFile a, EditLogFile b) {
        return ComparisonChain.start()
        .compare(a.getFirstTxId(), b.getFirstTxId())
        .compare(a.getLastTxId(), b.getLastTxId())
        .result();
      }
    };

    EditLogFile(File file,
        long firstTxId, long lastTxId) {
      this(file, firstTxId, lastTxId, false);
      assert (lastTxId != HdfsConstants.INVALID_TXID)
        && (lastTxId >= firstTxId);
    }
    
    EditLogFile(File file, long firstTxId, 
                long lastTxId, boolean isInProgress) { 
      assert (lastTxId == HdfsConstants.INVALID_TXID && isInProgress)
        || (lastTxId != HdfsConstants.INVALID_TXID && lastTxId >= firstTxId);
      assert (firstTxId > 0) || (firstTxId == HdfsConstants.INVALID_TXID);
      assert file != null;
      
      Preconditions.checkArgument(!isInProgress ||
          lastTxId == HdfsConstants.INVALID_TXID);
      
      this.firstTxId = firstTxId;
      this.lastTxId = lastTxId;
      this.file = file;
      this.isInProgress = isInProgress;
    }
    
    public long getFirstTxId() {
      return firstTxId;
    }
    
    public long getLastTxId() {
      return lastTxId;
    }
    
    boolean containsTxId(long txId) {
      return firstTxId <= txId && txId <= lastTxId;
    }

    /** 
     * Find out where the edit log ends.
     * This will update the lastTxId of the EditLogFile or
     * mark it as corrupt if it is.
     */
    public void validateLog() throws IOException {
      EditLogValidation val = EditLogFileInputStream.validateEditLog(file);
      this.lastTxId = val.getEndTxId();
      this.hasCorruptHeader = val.hasCorruptHeader();
    }

    public void scanLog() throws IOException {
      EditLogValidation val = EditLogFileInputStream.scanEditLog(file);
      this.lastTxId = val.getEndTxId();
      this.hasCorruptHeader = val.hasCorruptHeader();
    }

    public boolean isInProgress() {
      return isInProgress;
    }

    public File getFile() {
      return file;
    }
    
    boolean hasCorruptHeader() {
      return hasCorruptHeader;
    }

    void moveAsideCorruptFile() throws IOException {
      assert hasCorruptHeader;
      renameSelf(".corrupt");
    }

    void moveAsideTrashFile(long markerTxid) throws IOException {
      assert this.getFirstTxId() >= markerTxid;
      renameSelf(".trash");
    }

    public void moveAsideEmptyFile() throws IOException {
      assert lastTxId == HdfsConstants.INVALID_TXID;
      renameSelf(".empty");
    }
      
    private void renameSelf(String newSuffix) throws IOException {
      File src = file;
      File dst = new File(src.getParent(), src.getName() + newSuffix);
      // renameTo fails on Windows if the destination file already exists.
      try {
        if (dst.exists()) {
          if (!dst.delete()) {
            throw new IOException("Couldn't delete " + dst);
          }
        }
        NativeIO.renameTo(src, dst);
      } catch (IOException e) {
        throw new IOException(
            "Couldn't rename log " + src + " to " + dst, e);
      }
      file = dst;
    }

    @Override
    public String toString() {
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
                           +"inProgress=%b,hasCorruptHeader=%b)",
                           file.toString(), firstTxId, lastTxId,
                           isInProgress(), hasCorruptHeader);
    }
  }

  @Override
  public void discardSegments(long startTxid) throws IOException {
    discardEditLogSegments(startTxid);
  }
  
  @Override
  public void doPreUpgrade() throws IOException {
    LOG.info("Starting upgrade of edits directory " + sd.getRoot());
    try {
     NNUpgradeUtil.doPreUpgrade(conf, sd);
    } catch (IOException ioe) {
     LOG.error("Failed to move aside pre-upgrade storage " +
         "in image directory " + sd.getRoot(), ioe);
     throw ioe;
    }
  }
  
  /**
   * This method assumes that the fields of the {@link Storage} object have
   * already been updated to the appropriate new values for the upgrade.
   */
  @Override
  public void doUpgrade(Storage storage) throws IOException {
    NNUpgradeUtil.doUpgrade(sd, storage);
  }
  
  @Override
  public void doFinalize() throws IOException {
    NNUpgradeUtil.doFinalize(sd);
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    return NNUpgradeUtil.canRollBack(sd, storage,
        prevStorage, targetLayoutVersion);
  }

  @Override
  public void doRollback() throws IOException {
    NNUpgradeUtil.doRollBack(sd);
  }

  @Override
  public long getJournalCTime() throws IOException {
    StorageInfo sInfo = new StorageInfo((NodeType)null);
    sInfo.readProperties(sd);
    return sInfo.getCTime();
  }
}
