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

import static org.apache.hadoop.util.Time.now;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Time;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * FSImage handles checkpointing and logging of the namespace edits.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSImage implements Closeable {
  public static final Log LOG = LogFactory.getLog(FSImage.class.getName());

  protected FSEditLog editLog = null;
  private boolean isUpgradeFinalized = false;

  protected NNStorage storage;
  
  /**
   * The last transaction ID that was either loaded from an image
   * or loaded by loading edits files.
   */
  protected long lastAppliedTxId = 0;

  final private Configuration conf;

  protected NNStorageRetentionManager archivalManager;

  /* Used to make sure there are no concurrent checkpoints for a given txid
   * The checkpoint here could be one of the following operations.
   * a. checkpoint when NN is in standby.
   * b. admin saveNameSpace operation.
   * c. download checkpoint file from any remote checkpointer.
  */
  private final Set<Long> currentlyCheckpointing =
      Collections.<Long>synchronizedSet(new HashSet<Long>());

  /**
   * Construct an FSImage
   * @param conf Configuration
   * @throws IOException if default directories are invalid.
   */
  public FSImage(Configuration conf) throws IOException {
    this(conf,
         FSNamesystem.getNamespaceDirs(conf),
         FSNamesystem.getNamespaceEditsDirs(conf));
  }

  /**
   * Construct the FSImage. Set the default checkpoint directories.
   *
   * Setup storage and initialize the edit log.
   *
   * @param conf Configuration
   * @param imageDirs Directories the image can be stored in.
   * @param editsDirs Directories the editlog can be stored in.
   * @throws IOException if directories are invalid.
   */
  protected FSImage(Configuration conf,
                    Collection<URI> imageDirs,
                    List<URI> editsDirs)
      throws IOException {
    this.conf = conf;
    //初始化NNStorage用来管理Image和EditLog目录
    storage = new NNStorage(conf, imageDirs, editsDirs);
    if(conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY,
                       DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_DEFAULT)) {
      storage.setRestoreFailedStorage(true);
    }
    //用来对Edit文件进行管理，这里的Edit文件，
    // 包括了NameNode的本地EditLog文件和存放在远程的JournalNode上的EditLog文件
    this.editLog = new FSEditLog(conf, storage, editsDirs);
    //注意：editsDirs包含了本地的editsLog文件目录和远程的有3个以上的JournalNode组成的Quorum
    
    archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
  }
 
  void format(FSNamesystem fsn, String clusterId) throws IOException {
    long fileCount = fsn.getTotalFiles();
    // Expect 1 file, which is the root inode
    Preconditions.checkState(fileCount == 1,
        "FSImage.format should be called with an uninitialized namesystem, has " +
        fileCount + " files");
    NamespaceInfo ns = NNStorage.newNamespaceInfo();
    LOG.info("Allocated new BlockPoolId: " + ns.getBlockPoolID());
    ns.clusterID = clusterId;
    
    storage.format(ns);
    editLog.formatNonFileJournals(ns);
    saveFSImageInAllDirs(fsn, 0);
  }
  
  /**
   * Check whether the storage directories and non-file journals exist.
   * If running in interactive mode, will prompt the user for each
   * directory to allow them to format anyway. Otherwise, returns
   * false, unless 'force' is specified.
   * 
   * @param force if true, format regardless of whether dirs exist
   * @param interactive prompt the user when a dir exists
   * @return true if formatting should proceed
   * @throws IOException if some storage cannot be accessed
   */
  boolean confirmFormat(boolean force, boolean interactive) throws IOException {
    List<FormatConfirmable> confirms = Lists.newArrayList();
    for (StorageDirectory sd : storage.dirIterable(null)) {
      confirms.add(sd);
    }
    
    confirms.addAll(editLog.getFormatConfirmables());
    return Storage.confirmFormat(confirms, force, interactive);
  }
  
  /**
   * Analyze storage directories.
   * Recover from previous transitions if required. 
   * Perform fs state transition if necessary depending on the namespace info.
   * Read storage info. 
   * 
   * @throws IOException
   * @return true if the image needs to be saved or false otherwise
   *
   *
   * 这个方法先对保存命名空间镜像和保存编辑日志的目录进行分析，
   * 如果目录处于非正常状态，先对其进行恢复，
   * 然后查看节点状态，如果没有格式化则系统帮助我们格式化（先删除 后 创建）
   * 然后再调用FSImage.loadFSImage()方法将镜像和编辑日志数据导入到内存中
   */
  boolean recoverTransitionRead(StartupOption startOpt, FSNamesystem target,
      MetaRecoveryContext recovery)
      throws IOException {

    assert startOpt != StartupOption.FORMAT : 
      "NameNode formatting should be performed before reading the image";
    //fsimage和editlog目录
    Collection<URI> imageDirs = storage.getImageDirectories();
    Collection<URI> editsDirs = editLog.getEditURIs();
    // 所有数据都不存在。 如若 fsimage和editlog都不存在 则抛出异常
    if((imageDirs.size() == 0 || editsDirs.size() == 0)
                             && startOpt != StartupOption.IMPORT)  
      throw new IOException(
          "All specified directories are not accessible or do not exist.");

    Map<StorageDirectory, StorageState> dataDirStates =
             new HashMap<StorageDirectory, StorageState>();
    //检查节点的情况，如果需要恢复则进行doRecover ， 最后整体方法返回当前节点是否被格式化
    boolean isFormatted = recoverStorageDirs(startOpt, dataDirStates);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Data dir states:\n  " +
        Joiner.on("\n  ").withKeyValueSeparator(": ")
        .join(dataDirStates));
    }
    
    if (!isFormatted && startOpt != StartupOption.ROLLBACK 
                     && startOpt != StartupOption.IMPORT) {
      throw new IOException("NameNode is not formatted.");      
    }


    int layoutVersion = storage.getLayoutVersion();
    if (startOpt == StartupOption.METADATAVERSION) {
      System.out.println("HDFS Image Version: " + layoutVersion);
      System.out.println("Software format version: " +
        HdfsConstants.NAMENODE_LAYOUT_VERSION);
      return false;
    }

    if (layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION) {
      NNStorage.checkVersionUpgradable(storage.getLayoutVersion());
    }
    if (startOpt != StartupOption.UPGRADE
        && startOpt != StartupOption.UPGRADEONLY
        && !RollingUpgradeStartupOption.STARTED.matches(startOpt)
        && layoutVersion < Storage.LAST_PRE_UPGRADE_LAYOUT_VERSION
        && layoutVersion != HdfsConstants.NAMENODE_LAYOUT_VERSION) {
      throw new IOException(
          "\nFile system image contains an old layout version " 
          + storage.getLayoutVersion() + ".\nAn upgrade to version "
          + HdfsConstants.NAMENODE_LAYOUT_VERSION + " is required.\n"
          + "Please restart NameNode with the \""
          + RollingUpgradeStartupOption.STARTED.getOptionString()
          + "\" option if a rolling upgrade is already started;"
          + " or restart NameNode with the \""
          + StartupOption.UPGRADE.getName() + "\" option to start"
          + " a new upgrade.");
    }
    
    storage.processStartupOptionsForUpgrade(startOpt, layoutVersion);

    // 2. Format unformatted dirs.
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();//拿到元数据目录
      StorageState curState = dataDirStates.get(sd);//查看当前节点状态（NORMAL）
      switch(curState) {
      case NON_EXISTENT://存储不存在，抛异常
        throw new IOException(StorageState.NON_EXISTENT + 
                              " state cannot be here");
      case NOT_FORMATTED://未格式化，直接删掉目录，然后在创建
        LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
        LOG.info("Formatting ...");
        sd.clearDirectory(); // create empty currrent dir
        break;
      default:
        break;
      }
    }

    // 3.根据当前状态（REGULAR），走对应流程。如果一切正常则loadFSImage
    switch(startOpt) {
    case UPGRADE:
    case UPGRADEONLY:
      doUpgrade(target);
      return false; // upgrade saved image already
    case IMPORT:
      doImportCheckpoint(target);
      return false; // import checkpoint saved image already
    case ROLLBACK:
      throw new AssertionError("Rollback is now a standalone command, "
          + "NameNode should not be starting with this option.");
    case REGULAR:
    default:
      // just load the image
    }
    //TODO
    return loadFSImage(target, startOpt, recovery);
  }
  
  /**
   * For each storage directory, performs recovery of incomplete transitions
   * (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
   * state into the dataDirStates map.
   * @param dataDirStates output of storage directory states
   * @return true if there is at least one valid formatted storage directory
   */
  private boolean recoverStorageDirs(StartupOption startOpt,
      Map<StorageDirectory, StorageState> dataDirStates) throws IOException {
    boolean isFormatted = false;
    // This loop needs to be over all storage dirs, even shared dirs, to make
    // sure that we properly examine their state, but we make sure we don't
    // mutate the shared dir below in the actual loop.
    for (Iterator<StorageDirectory> it = 
                      storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      if (startOpt == StartupOption.METADATAVERSION) {
        /* All we need is the layout version. */
        storage.readProperties(sd);
        return true;
      }

      try {
        //检查节点状态（正常、不正常、性能恢复）
        curState = sd.analyzeStorage(startOpt, storage);
        // sd is locked but not opened
        switch(curState) {
        case NON_EXISTENT:
          // name-node fails if any of the configured storage dirs are missing
          throw new InconsistentFSStateException(sd.getRoot(),
                      "storage directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          break;
        case NORMAL:
          break;
        default:  // recovery is possible
          sd.doRecover(curState);
        }
        if (curState != StorageState.NOT_FORMATTED 
            && startOpt != StartupOption.ROLLBACK) {
          // read and verify consistency with other directories
          storage.readProperties(sd, startOpt);
          isFormatted = true;
        }
        if (startOpt == StartupOption.IMPORT && isFormatted)
          // import of a checkpoint is allowed only into empty image directories
          throw new IOException("Cannot import image from a checkpoint. " 
              + " NameNode already contains an image in " + sd.getRoot());
      } catch (IOException ioe) {
        sd.unlock();
        throw ioe;
      }
      dataDirStates.put(sd,curState);
    }
    return isFormatted;
  }

  /** Check if upgrade is in progress. */
  void checkUpgrade(FSNamesystem target) throws IOException {
    // Upgrade or rolling upgrade is allowed only if there are 
    // no previous fs states in any of the directories
    for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
      StorageDirectory sd = it.next();
      if (sd.getPreviousDir().exists())
        throw new InconsistentFSStateException(sd.getRoot(),
            "previous fs state should not exist during upgrade. "
            + "Finalize or rollback first.");
    }
  }

  /**
   * @return true if there is rollback fsimage (for rolling upgrade) in NameNode
   * directory.
   */
  public boolean hasRollbackFSImage() throws IOException {
    final FSImageStorageInspector inspector = new FSImageTransactionalStorageInspector(
        EnumSet.of(NameNodeFile.IMAGE_ROLLBACK));
    storage.inspectStorageDirs(inspector);
    try {
      List<FSImageFile> images = inspector.getLatestImages();
      return images != null && !images.isEmpty();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  void doUpgrade(FSNamesystem target) throws IOException {
    checkUpgrade(target);

    // load the latest image
    this.loadFSImage(target, StartupOption.UPGRADE, null);

    // Do upgrade for each directory
    target.checkRollingUpgrade("upgrade namenode");
    
    long oldCTime = storage.getCTime();
    storage.cTime = now();  // generate new cTime for the state
    int oldLV = storage.getLayoutVersion();
    storage.layoutVersion = HdfsConstants.NAMENODE_LAYOUT_VERSION;
    
    List<StorageDirectory> errorSDs =
      Collections.synchronizedList(new ArrayList<StorageDirectory>());
    assert !editLog.isSegmentOpen() : "Edits log must not be open.";
    LOG.info("Starting upgrade of local storage directories."
        + "\n   old LV = " + oldLV
        + "; old CTime = " + oldCTime
        + ".\n   new LV = " + storage.getLayoutVersion()
        + "; new CTime = " + storage.getCTime());
    // Do upgrade for each directory
    for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        NNUpgradeUtil.doPreUpgrade(conf, sd);
      } catch (Exception e) {
        LOG.error("Failed to move aside pre-upgrade storage " +
            "in image directory " + sd.getRoot(), e);
        errorSDs.add(sd);
        continue;
      }
    }
    if (target.isHaEnabled()) {
      editLog.doPreUpgradeOfSharedLog();
    }
    storage.reportErrorsOnDirectories(errorSDs);
    errorSDs.clear();

    saveFSImageInAllDirs(target, editLog.getLastWrittenTxId());

    // upgrade shared edit storage first
    if (target.isHaEnabled()) {
      editLog.doUpgradeOfSharedLog();
    }
    for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
      StorageDirectory sd = it.next();
      try {
        NNUpgradeUtil.doUpgrade(sd, storage);
      } catch (IOException ioe) {
        errorSDs.add(sd);
        continue;
      }
    }
    storage.reportErrorsOnDirectories(errorSDs);
    
    isUpgradeFinalized = false;
    if (!storage.getRemovedStorageDirs().isEmpty()) {
      // during upgrade, it's a fatal error to fail any storage directory
      throw new IOException("Upgrade failed in "
          + storage.getRemovedStorageDirs().size()
          + " storage directory(ies), previously logged.");
    }
  }

  void doRollback(FSNamesystem fsns) throws IOException {
    // Rollback is allowed only if there is 
    // a previous fs states in at least one of the storage directories.
    // Directories that don't have previous state do not rollback
    boolean canRollback = false;
    FSImage prevState = new FSImage(conf);
    try {
      prevState.getStorage().layoutVersion = HdfsConstants.NAMENODE_LAYOUT_VERSION;
      for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
        StorageDirectory sd = it.next();
        if (!NNUpgradeUtil.canRollBack(sd, storage, prevState.getStorage(),
            HdfsConstants.NAMENODE_LAYOUT_VERSION)) {
          continue;
        }
        LOG.info("Can perform rollback for " + sd);
        canRollback = true;
      }
      
      if (fsns.isHaEnabled()) {
        // If HA is enabled, check if the shared log can be rolled back as well.
        editLog.initJournalsForWrite();
        boolean canRollBackSharedEditLog = editLog.canRollBackSharedLog(
            prevState.getStorage(), HdfsConstants.NAMENODE_LAYOUT_VERSION);
        if (canRollBackSharedEditLog) {
          LOG.info("Can perform rollback for shared edit log.");
          canRollback = true;
        }
      }
      
      if (!canRollback)
        throw new IOException("Cannot rollback. None of the storage "
            + "directories contain previous fs state.");
  
      // Now that we know all directories are going to be consistent
      // Do rollback for each directory containing previous state
      for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
        StorageDirectory sd = it.next();
        LOG.info("Rolling back storage directory " + sd.getRoot()
                 + ".\n   new LV = " + prevState.getStorage().getLayoutVersion()
                 + "; new CTime = " + prevState.getStorage().getCTime());
        NNUpgradeUtil.doRollBack(sd);
      }
      if (fsns.isHaEnabled()) {
        // If HA is enabled, try to roll back the shared log as well.
        editLog.doRollback();
      }
      
      isUpgradeFinalized = true;
    } finally {
      prevState.close();
    }
  }

  /**
   * Load image from a checkpoint directory and save it into the current one.
   * @param target the NameSystem to import into
   * @throws IOException
   */
  void doImportCheckpoint(FSNamesystem target) throws IOException {
    Collection<URI> checkpointDirs =
      FSImage.getCheckpointDirs(conf, null);
    List<URI> checkpointEditsDirs =
      FSImage.getCheckpointEditsDirs(conf, null);

    if (checkpointDirs == null || checkpointDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }
    
    if (checkpointEditsDirs == null || checkpointEditsDirs.isEmpty()) {
      throw new IOException("Cannot import image from a checkpoint. "
                            + "\"dfs.namenode.checkpoint.dir\" is not set." );
    }

    FSImage realImage = target.getFSImage();
    FSImage ckptImage = new FSImage(conf, 
                                    checkpointDirs, checkpointEditsDirs);
    // load from the checkpoint dirs
    try {
      ckptImage.recoverTransitionRead(StartupOption.REGULAR, target, null);
    } finally {
      ckptImage.close();
    }
    // return back the real image
    realImage.getStorage().setStorageInfo(ckptImage.getStorage());
    realImage.getEditLog().setNextTxId(ckptImage.getEditLog().getLastWrittenTxId()+1);
    realImage.initEditLog(StartupOption.IMPORT);

    realImage.getStorage().setBlockPoolID(ckptImage.getBlockPoolID());

    // and save it but keep the same checkpointTime
    saveNamespace(target);
    getStorage().writeAll();
  }
  
  void finalizeUpgrade(boolean finalizeEditLog) throws IOException {
    LOG.info("Finalizing upgrade for local dirs. " +
        (storage.getLayoutVersion() == 0 ? "" : 
          "\n   cur LV = " + storage.getLayoutVersion()
          + "; cur CTime = " + storage.getCTime()));
    for (Iterator<StorageDirectory> it = storage.dirIterator(false); it.hasNext();) {
      StorageDirectory sd = it.next();
      NNUpgradeUtil.doFinalize(sd);
    }
    if (finalizeEditLog) {
      // We only do this in the case that HA is enabled and we're active. In any
      // other case the NN will have done the upgrade of the edits directories
      // already by virtue of the fact that they're local.
      editLog.doFinalizeOfSharedLog();
    }
    isUpgradeFinalized = true;
  }

  boolean isUpgradeFinalized() {
    return isUpgradeFinalized;
  }

  public FSEditLog getEditLog() {
    return editLog;
  }

  void openEditLogForWrite() throws IOException {
    assert editLog != null : "editLog must be initialized";
    editLog.openForWrite();
    storage.writeTransactionIdFileToStorage(editLog.getCurSegmentTxId());
  }
  
  /**
   * Toss the current image and namesystem, reloading from the specified
   * file.
   */
  void reloadFromImageFile(File file, FSNamesystem target) throws IOException {
    target.clear();
    LOG.debug("Reloading namespace from " + file);
    loadFSImage(file, target, null, false);
  }

  /**
   * 从一个目录中选择最新的镜像 加载它并和编辑日志进行合并
   *
   *
   * @return whether the image should be saved
   * @throws IOException
   */
  private boolean loadFSImage(FSNamesystem target, StartupOption startOpt,
      MetaRecoveryContext recovery)
      throws IOException {
    // 看启动项是否是 滚动升级的回滚模式
    final boolean rollingRollback
        = RollingUpgradeStartupOption.ROLLBACK.matches(startOpt);
    final EnumSet<NameNodeFile> nnfs;
    if (rollingRollback) {//如果是滚动升级的回滚，只需要从回滚映像加载。
      nnfs = EnumSet.of(NameNodeFile.IMAGE_ROLLBACK);
    } else {//否则，我们可以从IMAGE和IMAGE_ROLLBACK加载
      nnfs = EnumSet.of(NameNodeFile.IMAGE, NameNodeFile.IMAGE_ROLLBACK);
    }
    final FSImageStorageInspector inspector = storage.readAndInspectDirs(nnfs, startOpt);

    isUpgradeFinalized = inspector.isUpgradeFinalized();
    //获取最新的image文件
    List<FSImageFile> imageFiles = inspector.getLatestImages();
    //用于报告namenode启动进度的对象
    StartupProgress prog = NameNode.getStartupProgress();
    prog.beginPhase(Phase.LOADING_FSIMAGE);
    File phaseFile = imageFiles.get(0).getFile();
    //路径和长度也写到StartupProgress中
    prog.setFile(Phase.LOADING_FSIMAGE, phaseFile.getAbsolutePath());
    prog.setSize(Phase.LOADING_FSIMAGE, phaseFile.length());
    //如果目录处于这样的状态，则图像应该被重新保存。
    boolean needToSave = inspector.needToSave();

    Iterable<EditLogInputStream> editStreams = null;
    //初始化Editlog
    initEditLog(startOpt);
    //NN存储中的文件名是基于事务id的。
    //如果给定的布局版本中支持给定的特性，则返回true
    if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
      //如果我们对写开放，我们要么是非ha，要么是活动的NN，所以我们最好能够加载所有的编辑。
      // 如果我们是备用的NN，现在还不能读取所有的编辑。与此同时，对于HA升级，我们仍然会编写editlog，
      // 因此需要将这个toattrixid设置为max-seen txid。
      // 对于滚动升级的回滚，我们需要在升级标记之前将toat最少txid设置为txid。
      long toAtLeastTxId = editLog.isOpenForWrite() ? inspector
          .getMaxSeenTxId() : 0;
      if (rollingRollback) {
        // note that the first image in imageFiles is the special checkpoint
        // for the rolling upgrade
        //请注意，imageFiles中的第一个映像是滚动升级的特殊检查点。
        toAtLeastTxId = imageFiles.get(0).getCheckpointTxId() + 2;
      }
      editStreams = editLog.selectInputStreams(
          imageFiles.get(0).getCheckpointTxId() + 1,
          toAtLeastTxId, recovery, false);
    } else {
      editStreams = FSImagePreTransactionalStorageInspector
        .getEditLogStreams(storage);
    }
    //返回我们将用于输入的最大操作码大小。
    int maxOpSize = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT);
    for (EditLogInputStream elis : editStreams) {
      elis.setMaxOpSize(maxOpSize);
    }
 
    for (EditLogInputStream l : editStreams) {
      LOG.debug("Planning to load edit log stream: " + l);
    }
    if (!editStreams.iterator().hasNext()) {
      LOG.info("No edit log streams selected.");
    }
    
    FSImageFile imageFile = null;
    for (int i = 0; i < imageFiles.size(); i++) {
      try {
        imageFile = imageFiles.get(i);
        //TODO
        loadFSImageFile(target, recovery, imageFile, startOpt);
        break;
      } catch (IOException ioe) {
        LOG.error("Failed to load image from " + imageFile, ioe);
        target.clear();
        imageFile = null;
      }
    }
    // Failed to load any images, error out
    if (imageFile == null) {
      FSEditLog.closeAllStreams(editStreams);
      throw new IOException("Failed to load an FSImage file!");
    }
    prog.endPhase(Phase.LOADING_FSIMAGE);
    
    if (!rollingRollback) {
      long txnsAdvanced = loadEdits(editStreams, target, startOpt, recovery);
      needToSave |= needsResaveBasedOnStaleCheckpoint(imageFile.getFile(),
          txnsAdvanced);
      if (RollingUpgradeStartupOption.DOWNGRADE.matches(startOpt)) {
        // rename rollback image if it is downgrade
        renameCheckpoint(NameNodeFile.IMAGE_ROLLBACK, NameNodeFile.IMAGE);
      }
    } else {
      // Trigger the rollback for rolling upgrade. Here lastAppliedTxId equals
      // to the last txid in rollback fsimage.
      rollingRollback(lastAppliedTxId + 1, imageFiles.get(0).getCheckpointTxId());
      needToSave = false;
    }
    editLog.setNextTxId(lastAppliedTxId + 1);
    return needToSave;
  }

  /** rollback for rolling upgrade. */
  private void rollingRollback(long discardSegmentTxId, long ckptId)
      throws IOException {
    // discard discard unnecessary editlog segments starting from the given id
    this.editLog.discardSegments(discardSegmentTxId);
    // rename the special checkpoint
    renameCheckpoint(ckptId, NameNodeFile.IMAGE_ROLLBACK, NameNodeFile.IMAGE,
        true);
    // purge all the checkpoints after the marker
    archivalManager.purgeCheckpoinsAfter(NameNodeFile.IMAGE, ckptId);
    String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
    if (HAUtil.isHAEnabled(conf, nameserviceId)) {
      // close the editlog since it is currently open for write
      this.editLog.close();
      // reopen the editlog for read
      this.editLog.initSharedJournalsForRead();
    }
  }

  void loadFSImageFile(FSNamesystem target, MetaRecoveryContext recovery,
      FSImageFile imageFile, StartupOption startupOption) throws IOException {
    LOG.debug("Planning to load image :\n" + imageFile);
    StorageDirectory sdForProperties = imageFile.sd;
    storage.readProperties(sdForProperties, startupOption);

    /**
     * 以下是对fsimage数据的读取 有三个判断 看符合哪一个 重重重
     */
    if (NameNodeLayoutVersion.supports(//如果 NN存储中的文件名是基于事务id的 走这个方法
        LayoutVersion.Feature.TXID_BASED_LAYOUT, getLayoutVersion())) {
      //对于基于txid的布局，我们应该在图像文件旁边有一个.md5文件
      boolean isRollingRollback = RollingUpgradeStartupOption.ROLLBACK
          .matches(startupOption);
      loadFSImage(imageFile.getFile(), target, recovery, isRollingRollback);
    } else if (NameNodeLayoutVersion.supports(
        LayoutVersion.Feature.FSIMAGE_CHECKSUM, getLayoutVersion())) {
      //支持校验和fsimage
      String md5 = storage.getDeprecatedProperty(
          NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY);
      if (md5 == null) {
        throw new InconsistentFSStateException(sdForProperties.getRoot(),
            "Message digest property " +
            NNStorage.DEPRECATED_MESSAGE_DIGEST_PROPERTY +
            " not set for storage directory " + sdForProperties.getRoot());
      }
      //TODO
      loadFSImage(imageFile.getFile(), new MD5Hash(md5), target, recovery,
          false);
    } else {
      // We don't have any record of the md5sum
      //TODO
      loadFSImage(imageFile.getFile(), null, target, recovery, false);
    }
  }

  public void initEditLog(StartupOption startOpt) throws IOException {
    Preconditions.checkState(getNamespaceID() != 0,
        "Must know namespace ID before initting edit log");
    String nameserviceId = DFSUtil.getNamenodeNameServiceId(conf);
    if (!HAUtil.isHAEnabled(conf, nameserviceId)) {
      // If this NN is not HA
      editLog.initJournalsForWrite();
      editLog.recoverUnclosedStreams();
    } else if (HAUtil.isHAEnabled(conf, nameserviceId)
        && (startOpt == StartupOption.UPGRADE
            || startOpt == StartupOption.UPGRADEONLY
            || RollingUpgradeStartupOption.ROLLBACK.matches(startOpt))) {
      // This NN is HA, but we're doing an upgrade or a rollback of rolling
      // upgrade so init the edit log for write.
      editLog.initJournalsForWrite();
      if (startOpt == StartupOption.UPGRADE
          || startOpt == StartupOption.UPGRADEONLY) {
        long sharedLogCTime = editLog.getSharedLogCTime();
        if (this.storage.getCTime() < sharedLogCTime) {
          throw new IOException("It looks like the shared log is already " +
              "being upgraded but this NN has not been upgraded yet. You " +
              "should restart this NameNode with the '" +
              StartupOption.BOOTSTRAPSTANDBY.getName() + "' option to bring " +
              "this NN in sync with the other.");
        }
      }
      editLog.recoverUnclosedStreams();
    } else {
      // This NN is HA and we're not doing an upgrade.
      editLog.initSharedJournalsForRead();
    }
  }

  /**
   * @param imageFile the image file that was loaded
   * @param numEditsLoaded the number of edits loaded from edits logs
   * @return true if the NameNode should automatically save the namespace
   * when it is started, due to the latest checkpoint being too old.
   */
  private boolean needsResaveBasedOnStaleCheckpoint(
      File imageFile, long numEditsLoaded) {
    final long checkpointPeriod = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT);
    final long checkpointTxnCount = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    long checkpointAge = Time.now() - imageFile.lastModified();

    return (checkpointAge > checkpointPeriod * 1000) ||
           (numEditsLoaded > checkpointTxnCount);
  }
  
  /**
   * Load the specified list of edit files into the image.
   */
  public long loadEdits(Iterable<EditLogInputStream> editStreams,
      FSNamesystem target) throws IOException {
    return loadEdits(editStreams, target, null, null);
  }

  private long loadEdits(Iterable<EditLogInputStream> editStreams,
      FSNamesystem target, StartupOption startOpt, MetaRecoveryContext recovery)
      throws IOException {
    LOG.debug("About to load edits:\n  " + Joiner.on("\n  ").join(editStreams));
    //StartupProgress 进度信息类
    StartupProgress prog = NameNode.getStartupProgress();
    //标识当前进度为加载edit log文件
    prog.beginPhase(Phase.LOADING_EDITS);
    //加载edit log最新事务ID
    long prevLastAppliedTxId = lastAppliedTxId;  
    try {
      //FSEditLogLoader负责对当前这一轮的多个stream(editStreams)进行读取并加载到内存
      FSEditLogLoader loader = new FSEditLogLoader(target, lastAppliedTxId);
      
      // Load latest edits
      for (EditLogInputStream editIn : editStreams) {
        LOG.info("Reading " + editIn + " expecting start txid #" +
              (lastAppliedTxId + 1));
        try {
          //TODO
          //loader会不断从远程读取新的op，同时从op中提取txId来更新自己的txId，从而让StandbyNamenode的txid逐渐向前递增
          loader.loadFSEdits(editIn, lastAppliedTxId + 1, startOpt, recovery);
        } finally {
          //从loader中获取当前已经load过来的最新的txId,更新到FSImage对象,用lastAppliedTxId记录当前已经成功读取并加载的位置
          //这样，当EditLogTailer的下一轮运行开始的时候，就从这个lastAppliedTxId开始请求edit log文件，因为这个位置之前的操作已经完成了同步和加载。
          //对应---》FSEditLogLoader loader = new FSEditLogLoader(target, lastAppliedTxId);
          lastAppliedTxId = loader.getLastAppliedTxId();
        }
        // If we are in recovery mode, we may have skipped over some txids.
        if (editIn.getLastTxId() != HdfsConstants.INVALID_TXID) {
          lastAppliedTxId = editIn.getLastTxId();
        }
      }
    } finally {//关闭所有读取流
      FSEditLog.closeAllStreams(editStreams);
      // update the counts
      updateCountForQuota(target.getBlockManager().getStoragePolicySuite(),
          target.dir.rootDir);
    }
    prog.endPhase(Phase.LOADING_EDITS);
    return lastAppliedTxId - prevLastAppliedTxId;
  }

  /**
   * Update the count of each directory with quota in the namespace.
   * A directory's count is defined as the total number inodes in the tree
   * rooted at the directory.
   * 
   * This is an update of existing state of the filesystem and does not
   * throw QuotaExceededException.
   */
  static void updateCountForQuota(BlockStoragePolicySuite bsps,
                                  INodeDirectory root) {
    updateCountForQuotaRecursively(bsps, root.getStoragePolicyID(), root,
        new QuotaCounts.Builder().build());
 }

  private static void updateCountForQuotaRecursively(BlockStoragePolicySuite bsps,
      byte blockStoragePolicyId, INodeDirectory dir, QuotaCounts counts) {
    final long parentNamespace = counts.getNameSpace();
    final long parentStoragespace = counts.getStorageSpace();
    final EnumCounters<StorageType> parentTypeSpaces = counts.getTypeSpaces();

    dir.computeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId, counts);
    
    for (INode child : dir.getChildrenList(Snapshot.CURRENT_STATE_ID)) {
      final byte childPolicyId = child.getStoragePolicyIDForQuota(blockStoragePolicyId);
      if (child.isDirectory()) {
        updateCountForQuotaRecursively(bsps, childPolicyId,
            child.asDirectory(), counts);
      } else {
        // file or symlink: count here to reduce recursive calls.
        child.computeQuotaUsage(bsps, childPolicyId, counts, false,
            Snapshot.CURRENT_STATE_ID);
      }
    }
      
    if (dir.isQuotaSet()) {
      // check if quota is violated. It indicates a software bug.
      final QuotaCounts q = dir.getQuotaCounts();

      final long namespace = counts.getNameSpace() - parentNamespace;
      final long nsQuota = q.getNameSpace();
      if (Quota.isViolated(nsQuota, namespace)) {
        LOG.warn("Namespace quota violation in image for "
            + dir.getFullPathName()
            + " quota = " + nsQuota + " < consumed = " + namespace);
      }

      final long ssConsumed = counts.getStorageSpace() - parentStoragespace;
      final long ssQuota = q.getStorageSpace();
      if (Quota.isViolated(ssQuota, ssConsumed)) {
        LOG.warn("Storagespace quota violation in image for "
            + dir.getFullPathName()
            + " quota = " + ssQuota + " < consumed = " + ssConsumed);
      }

      final EnumCounters<StorageType> typeSpaces =
          new EnumCounters<StorageType>(StorageType.class);
      for (StorageType t : StorageType.getTypesSupportingQuota()) {
        final long typeSpace = counts.getTypeSpaces().get(t) -
            parentTypeSpaces.get(t);
        final long typeQuota = q.getTypeSpaces().get(t);
        if (Quota.isViolated(typeQuota, typeSpace)) {
          LOG.warn("Storage type quota violation in image for "
              + dir.getFullPathName()
              + " type = " + t.toString() + " quota = "
              + typeQuota + " < consumed " + typeSpace);
        }
      }

      dir.getDirectoryWithQuotaFeature().setSpaceConsumed(namespace, ssConsumed,
          typeSpaces);
    }
  }

  /**
   * Load the image namespace from the given image file, verifying
   * it against the MD5 sum stored in its associated .md5 file.
   */
  private void loadFSImage(File imageFile, FSNamesystem target,
      MetaRecoveryContext recovery, boolean requireSameLayoutVersion)
      throws IOException {
    MD5Hash expectedMD5 = MD5FileUtils.readStoredMd5ForFile(imageFile);
    if (expectedMD5 == null) {
      throw new IOException("No MD5 file found corresponding to image file "
          + imageFile);
    }
    loadFSImage(imageFile, expectedMD5, target, recovery,
        requireSameLayoutVersion);
  }
  
  /**
   * 从文件加载文件系统映像。 这是一个很大的文件名和块列表。
   */
  private void loadFSImage(File curFile, MD5Hash expectedMd5,
      FSNamesystem target, MetaRecoveryContext recovery,
      boolean requireSameLayoutVersion) throws IOException {
    // // 当FsImageLoader加载滚动升级信息时，需要BlockPoolId。 确保ID已正确设置。
    target.setBlockPoolId(this.getBlockPoolID());
    //加载
    FSImageFormat.LoaderDelegator loader = FSImageFormat.newLoader(conf, target);
    loader.load(curFile, requireSameLayoutVersion);

    // //检查我们加载的镜像摘要是否符合我们的预期
    MD5Hash readImageMd5 = loader.getLoadedImageMd5();
    if (expectedMd5 != null &&
        !expectedMd5.equals(readImageMd5)) {
      throw new IOException("Image file " + curFile +
          " is corrupt with MD5 checksum of " + readImageMd5 +
          " but expecting " + expectedMd5);
    }

    long txId = loader.getLoadedImageTxId();
    LOG.info("Loaded image for txid " + txId + " from " + curFile);
    lastAppliedTxId = txId;
    storage.setMostRecentCheckpointInfo(txId, curFile.lastModified());
  }

  /**
   * Save the contents of the FS image to the file.
   * 从saveFSImage()方法可以看到，生成image文件的时候，并不是直接将内存镜像dump到对应的image磁盘目录，
   * 而是会产生一个以fsimage.ckpt开头的中间文件；
   * 如：fsimage.ckpt_0000000000992806947，
   * 然后生成对应的MD5校验文件，如：fsimage.ckpt_0000000000992806947.md5。
   *
   * 当多目录image文件全部完成了中间文件的生成，再调用renameCheckpoint(...)方法，
   * 将所有目录的中间文件rename为最终的格式为fsimage_0000000000992806947的文件；
   */
  void saveFSImage(SaveNamespaceContext context, StorageDirectory sd,
      NameNodeFile dstType) throws IOException {
    //checkpoint的目标偏移位置
    long txid = context.getTxId();
    //保存image文件
    File newFile = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
    //保存MD5文件
    File dstFile = NNStorage.getStorageFile(sd, dstType, txid);
    
    FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context);
    FSImageCompression compression = FSImageCompression.createCompression(conf);

    //按照压缩配置，将img存入到以fsimage.ckpt开头的中间文件中
    saver.save(newFile, compression);
    //保存md5文件
    MD5FileUtils.saveMD5File(dstFile, saver.getSavedDigest());
    //此处保存，是为了做checkpoint的时候，能知道上一次checkpoint的位置
    storage.setMostRecentCheckpointInfo(txid, Time.now());
  }

  /**
   * Save FSimage in the legacy format. This is not for NN consumption,
   * but for tools like OIV.
   */
  public void saveLegacyOIVImage(FSNamesystem source, String targetDir,
      Canceler canceler) throws IOException {
    FSImageCompression compression =
        FSImageCompression.createCompression(conf);
    long txid = getLastAppliedOrWrittenTxId();
    SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid,
        canceler);
    FSImageFormat.Saver saver = new FSImageFormat.Saver(ctx);
    String imageFileName = NNStorage.getLegacyOIVImageFileName(txid);
    File imageFile = new File(targetDir, imageFileName);
    saver.save(imageFile, compression);
    archivalManager.purgeOldLegacyOIVImages(targetDir, txid);
  }


  /**
   * FSImageSaver is being run in a separate thread when saving
   * FSImage. There is one thread per each copy of the image.
   *
   * FSImageSaver assumes that it was launched from a thread that holds
   * FSNamesystem lock and waits for the execution of FSImageSaver thread
   * to finish.
   * This way we are guaranteed that the namespace is not being updated
   * while multiple instances of FSImageSaver are traversing it
   * and writing it out.
   *
   *
   * FSImageSaver类负责完成对把img文件存放到某个img目录
   */
  private class FSImageSaver implements Runnable {
    private final SaveNamespaceContext context;
    private final StorageDirectory sd;
    private final NameNodeFile nnf;

    public FSImageSaver(SaveNamespaceContext context, StorageDirectory sd,
        NameNodeFile nnf) {
      this.context = context;
      this.sd = sd;
      this.nnf = nnf;
    }

    @Override
    public void run() {
      try {
        saveFSImage(context, sd, nnf);
      } catch (SaveNamespaceCancelledException snce) {
        LOG.info("Cancelled image saving for " + sd.getRoot() +
            ": " + snce.getMessage());
        // don't report an error on the storage dir!
      } catch (Throwable t) {
        LOG.error("Unable to save image for " + sd.getRoot(), t);
        context.reportErrorOnStorageDirectory(sd);
      }
    }
    
    @Override
    public String toString() {
      return "FSImageSaver for " + sd.getRoot() +
             " of type " + sd.getStorageDirType();
    }
  }
  
  private void waitForThreads(List<Thread> threads) {
    for (Thread thread : threads) {
      while (thread.isAlive()) {
        try {
          thread.join();
        } catch (InterruptedException iex) {
          LOG.error("Caught interrupted exception while waiting for thread " +
                    thread.getName() + " to finish. Retrying join");
        }        
      }
    }
  }
  
  /**
   * Update version of all storage directories.
   */
  public synchronized void updateStorageVersion() throws IOException {
    storage.writeAll();
  }

  /**
   * @see #saveNamespace(FSNamesystem, NameNodeFile, Canceler)
   */
  public synchronized void saveNamespace(FSNamesystem source)
      throws IOException {
    saveNamespace(source, NameNodeFile.IMAGE, null);
  }
  
  /**
   * Save the contents of the FS image to a new image file in each of the
   * current storage directories.
   */
  public synchronized void saveNamespace(FSNamesystem source, NameNodeFile nnf,
      Canceler canceler) throws IOException {
    assert editLog != null : "editLog must be initialized";
    LOG.info("Save namespace ...");
    storage.attemptRestoreRemovedStorage();

    boolean editLogWasOpen = editLog.isSegmentOpen();
    
    if (editLogWasOpen) {
      editLog.endCurrentLogSegment(true);
    }
    //获取editlog的最新事务id
    long imageTxId = getLastAppliedOrWrittenTxId();
    if (!addToCheckpointing(imageTxId)) {
      throw new IOException(
          "FS image is being downloaded from another NN at txid " + imageTxId);
    }
    try {
      try {
        //这个方法会生成image文件，并存放到一个或者多个image存储目录
        //通过dfs.namenode.name.dir配置
        saveFSImageInAllDirs(source, nnf, imageTxId, canceler);
        storage.writeAll();
      } finally {
        if (editLogWasOpen) {
          editLog.startLogSegment(imageTxId + 1, true);
          // Take this opportunity to note the current transaction.
          // Even if the namespace save was cancelled, this marker
          // is only used to determine what transaction ID is required
          // for startup. So, it doesn't hurt to update it unnecessarily.
          storage.writeTransactionIdFileToStorage(imageTxId + 1);
        }
      }
    } finally {
      removeFromCheckpointing(imageTxId);
    }
  }

  /**
   * @see #saveFSImageInAllDirs(FSNamesystem, NameNodeFile, long, Canceler)
   */
  protected synchronized void saveFSImageInAllDirs(FSNamesystem source, long txid)
      throws IOException {
    if (!addToCheckpointing(txid)) {
      throw new IOException(("FS image is being downloaded from another NN"));
    }
    try {
      saveFSImageInAllDirs(source, NameNodeFile.IMAGE, txid, null);
    } finally {
      removeFromCheckpointing(txid);
    }
  }

  public boolean addToCheckpointing(long txid) {
    return currentlyCheckpointing.add(txid);
  }

  public void removeFromCheckpointing(long txid) {
    currentlyCheckpointing.remove(txid);
  }

  private synchronized void saveFSImageInAllDirs(FSNamesystem source,
      NameNodeFile nnf, long txid, Canceler canceler) throws IOException {
    //记录状态
    StartupProgress prog = NameNode.getStartupProgress();
    prog.beginPhase(Phase.SAVING_CHECKPOINT);

    //获取配置的img 目录
    if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
      throw new IOException("No image directories available!");
    }
    if (canceler == null) {
      canceler = new Canceler();
    }
    //ctx中保存了生成img文件所需要的上下文信息，最重要的，这个img文件的end txId,即这个checkpoint的目标偏移位置
    SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid, canceler);
    
    try {
      List<Thread> saveThreads = new ArrayList<Thread>();
      // save images into current
      //对于每一个storage目录，都创建一个独立线程，同时进行img文件的生成，提高生成效率
      for (Iterator<StorageDirectory> it
             = storage.dirIterator(NameNodeDirType.IMAGE); it.hasNext();) {
        StorageDirectory sd = it.next();
        //fsimage.ckpt
        FSImageSaver saver = new FSImageSaver(ctx, sd, nnf);
        Thread saveThread = new Thread(saver, saver.toString());
        saveThreads.add(saveThread);
        saveThread.start();
      }
      //等待所有的saveThread完成存储操作
      waitForThreads(saveThreads);
      saveThreads.clear();
      //移除掉有错误的目录（FSImage里面的run，cache住的异常）
      storage.reportErrorsOnDirectories(ctx.getErrorSDs());
  
      if (storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0) {
        throw new IOException(
          "Failed to save in any storage directories while saving namespace.");
      }
      if (canceler.isCancelled()) {
        deleteCancelledCheckpoint(txid);
        ctx.checkCancelled(); // throws
        assert false : "should have thrown above!";
      }
      //完成了img文件的存取，实际上是存为一个中间文件，以NameNodeFile.IMAGE_NEW开头。
      //这时候就可以把这些中间文件rename称为最终正式文件了
      //最后一个参数false，代表不需要rename md5文件，这是因为在FSImageSaver中生成临时文件的时候已经生成了最终的md5文件
      renameCheckpoint(txid, NameNodeFile.IMAGE_NEW, nnf, false);
  
      // Since we now have a new checkpoint, we can clean up some
      // old edit logs and checkpoints.
      purgeOldStorage(nnf);
    } finally {
      // Notify any threads waiting on the checkpoint to be canceled
      // that it is complete.
      ctx.markComplete();
      ctx = null;
    }
    prog.endPhase(Phase.SAVING_CHECKPOINT);
  }

  /**
   * Purge any files in the storage directories that are no longer
   * necessary.
   */
  void purgeOldStorage(NameNodeFile nnf) {
    try {
      archivalManager.purgeOldStorage(nnf);
    } catch (Exception e) {
      LOG.warn("Unable to purge old storage " + nnf.getName(), e);
    }
  }

  /**
   * Rename FSImage with the specific txid
   */
  private void renameCheckpoint(long txid, NameNodeFile fromNnf,
      NameNodeFile toNnf, boolean renameMD5) throws IOException {
    ArrayList<StorageDirectory> al = null;

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      try {
        renameImageFileInDir(sd, fromNnf, toNnf, txid, renameMD5);
      } catch (IOException ioe) {
        LOG.warn("Unable to rename checkpoint in " + sd, ioe);
        if (al == null) {
          al = Lists.newArrayList();
        }
        al.add(sd);
      }
    }
    if(al != null) storage.reportErrorsOnDirectories(al);
  }

  /**
   * Rename all the fsimage files with the specific NameNodeFile type. The
   * associated checksum files will also be renamed.
   */
  void renameCheckpoint(NameNodeFile fromNnf, NameNodeFile toNnf)
      throws IOException {
    ArrayList<StorageDirectory> al = null;
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector(EnumSet.of(fromNnf));
    storage.inspectStorageDirs(inspector);
    for (FSImageFile image : inspector.getFoundImages()) {
      try {
        renameImageFileInDir(image.sd, fromNnf, toNnf, image.txId, true);
      } catch (IOException ioe) {
        LOG.warn("Unable to rename checkpoint in " + image.sd, ioe);
        if (al == null) {
          al = Lists.newArrayList();
        }
        al.add(image.sd);
      }
    }
    if(al != null) {
      storage.reportErrorsOnDirectories(al);
    }
  }

  /**
   * Deletes the checkpoint file in every storage directory,
   * since the checkpoint was cancelled.
   */
  private void deleteCancelledCheckpoint(long txid) throws IOException {
    ArrayList<StorageDirectory> al = Lists.newArrayList();

    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File ckpt = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE_NEW, txid);
      if (ckpt.exists() && !ckpt.delete()) {
        LOG.warn("Unable to delete cancelled checkpoint in " + sd);
        al.add(sd);            
      }
    }
    storage.reportErrorsOnDirectories(al);
  }

  private void renameImageFileInDir(StorageDirectory sd, NameNodeFile fromNnf,
      NameNodeFile toNnf, long txid, boolean renameMD5) throws IOException {
    final File fromFile = NNStorage.getStorageFile(sd, fromNnf, txid);
    final File toFile = NNStorage.getStorageFile(sd, toNnf, txid);
    // renameTo fails on Windows if the destination file already exists.
    if(LOG.isDebugEnabled()) {
      LOG.debug("renaming  " + fromFile.getAbsolutePath() 
                + " to " + toFile.getAbsolutePath());
    }
    if (!fromFile.renameTo(toFile)) {
      if (!toFile.delete() || !fromFile.renameTo(toFile)) {
        throw new IOException("renaming  " + fromFile.getAbsolutePath() + " to "  + 
            toFile.getAbsolutePath() + " FAILED");
      }
    }
    if (renameMD5) {
      MD5FileUtils.renameMD5File(fromFile, toFile);
    }
  }

  CheckpointSignature rollEditLog() throws IOException {

    getEditLog().rollEditLog();//开始双缓冲写日志

    //将当前事务ID记录到dfs/name/current/seen_txid里面,这个id就是当前edit文件的名称：fsimage_0000000000000000003
    storage.writeTransactionIdFileToStorage(getEditLog().getCurSegmentTxId());
    return new CheckpointSignature(this);
  }

  /**
   * Start checkpoint.
   * <p>
   * If backup storage contains image that is newer than or incompatible with 
   * what the active name-node has, then the backup node should shutdown.<br>
   * If the backup image is older than the active one then it should 
   * be discarded and downloaded from the active node.<br>
   * If the images are the same then the backup image will be used as current.
   * 
   * @param bnReg the backup node registration.
   * @param nnReg this (active) name-node registration.
   * @return {@link NamenodeCommand} if backup node should shutdown or
   * {@link CheckpointCommand} prescribing what backup node should 
   *         do with its image.
   * @throws IOException
   */
  NamenodeCommand startCheckpoint(NamenodeRegistration bnReg, // backup node
                                  NamenodeRegistration nnReg) // active name-node
  throws IOException {
    LOG.info("Start checkpoint at txid " + getEditLog().getLastWrittenTxId());
    String msg = null;
    // Verify that checkpoint is allowed
    if(bnReg.getNamespaceID() != storage.getNamespaceID())
      msg = "Name node " + bnReg.getAddress()
            + " has incompatible namespace id: " + bnReg.getNamespaceID()
            + " expected: " + storage.getNamespaceID();
    else if(bnReg.isRole(NamenodeRole.NAMENODE))
      msg = "Name node " + bnReg.getAddress()
            + " role " + bnReg.getRole() + ": checkpoint is not allowed.";
    else if(bnReg.getLayoutVersion() < storage.getLayoutVersion()
        || (bnReg.getLayoutVersion() == storage.getLayoutVersion()
            && bnReg.getCTime() > storage.getCTime()))
      // remote node has newer image age
      msg = "Name node " + bnReg.getAddress()
            + " has newer image layout version: LV = " +bnReg.getLayoutVersion()
            + " cTime = " + bnReg.getCTime()
            + ". Current version: LV = " + storage.getLayoutVersion()
            + " cTime = " + storage.getCTime();
    if(msg != null) {
      LOG.error(msg);
      return new NamenodeCommand(NamenodeProtocol.ACT_SHUTDOWN);
    }
    boolean needToReturnImg = true;
    if(storage.getNumStorageDirs(NameNodeDirType.IMAGE) == 0)
      // do not return image if there are no image directories
      needToReturnImg = false;
    CheckpointSignature sig = rollEditLog();
    return new CheckpointCommand(sig, needToReturnImg);
  }

  /**
   * End checkpoint.
   * <p>
   * Validate the current storage info with the given signature.
   * 
   * @param sig to validate the current storage info against
   * @throws IOException if the checkpoint fields are inconsistent
   */
  void endCheckpoint(CheckpointSignature sig) throws IOException {
    LOG.info("End checkpoint at txid " + getEditLog().getLastWrittenTxId());
    sig.validateStorageInfo(this);
  }

  /**
   * This is called by the 2NN after having downloaded an image, and by
   * the NN after having received a new image from the 2NN. It
   * renames the image from fsimage_N.ckpt to fsimage_N and also
   * saves the related .md5 file into place.
   */
  public synchronized void saveDigestAndRenameCheckpointImage(NameNodeFile nnf,
      long txid, MD5Hash digest) throws IOException {
    // Write and rename MD5 file
    List<StorageDirectory> badSds = Lists.newArrayList();
    
    for (StorageDirectory sd : storage.dirIterable(NameNodeDirType.IMAGE)) {
      File imageFile = NNStorage.getImageFile(sd, nnf, txid);
      try {
        MD5FileUtils.saveMD5File(imageFile, digest);
      } catch (IOException ioe) {
        badSds.add(sd);
      }
    }
    storage.reportErrorsOnDirectories(badSds);
    
    CheckpointFaultInjector.getInstance().afterMD5Rename();
    
    // Rename image from tmp file
    renameCheckpoint(txid, NameNodeFile.IMAGE_NEW, nnf, false);
    // So long as this is the newest image available,
    // advertise it as such to other checkpointers
    // from now on
    if (txid > storage.getMostRecentCheckpointTxId()) {
      storage.setMostRecentCheckpointInfo(txid, Time.now());
    }
  }

  @Override
  synchronized public void close() throws IOException {
    if (editLog != null) { // 2NN doesn't have any edit log
      getEditLog().close();
    }
    storage.close();
  }


  /**
   * Retrieve checkpoint dirs from configuration.
   *
   * @param conf the Configuration
   * @param defaultValue a default value for the attribute, if null
   * @return a Collection of URIs representing the values in 
   * dfs.namenode.checkpoint.dir configuration property
   */
  static Collection<URI> getCheckpointDirs(Configuration conf,
      String defaultValue) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY);
    if (dirNames.size() == 0 && defaultValue != null) {
      dirNames.add(defaultValue);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  static List<URI> getCheckpointEditsDirs(Configuration conf,
      String defaultName) {
    Collection<String> dirNames = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY);
    if (dirNames.size() == 0 && defaultName != null) {
      dirNames.add(defaultName);
    }
    return Util.stringCollectionAsURIs(dirNames);
  }

  public NNStorage getStorage() {
    return storage;
  }

  public int getLayoutVersion() {
    return storage.getLayoutVersion();
  }
  
  public int getNamespaceID() {
    return storage.getNamespaceID();
  }
  
  public String getClusterID() {
    return storage.getClusterID();
  }
  
  public String getBlockPoolID() {
    return storage.getBlockPoolID();
  }

  public synchronized long getLastAppliedTxId() {
    return lastAppliedTxId;
  }

  public long getLastAppliedOrWrittenTxId() {
    return Math.max(lastAppliedTxId,
        editLog != null ? editLog.getLastWrittenTxId() : 0);
  }

  public void updateLastAppliedTxIdFromWritten() {
    this.lastAppliedTxId = editLog.getLastWrittenTxId();
  }

  // Should be OK for this to not be synchronized since all of the places which
  // mutate this value are themselves synchronized so it shouldn't be possible
  // to see this flop back and forth. In the worst case this will just return an
  // old value.
  public long getMostRecentCheckpointTxId() {
    return storage.getMostRecentCheckpointTxId();
  }
}
