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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.base.Predicate;

/**
 * 
 * NameNodeResourceChecker provides a method -
 * <code>hasAvailableDiskSpace</code> - which will return true if and only if
 * the NameNode has disk space available on all required volumes, and any volume
 * which is configured to be redundant. Volumes containing file system edits dirs
 * are added by default, and arbitrary extra volumes may be configured as well.
 */
@InterfaceAudience.Private
public class NameNodeResourceChecker {
  private static final Log LOG = LogFactory.getLog(NameNodeResourceChecker.class.getName());

  // Space (in bytes) reserved per volume.
  private final long duReserved;

  private final Configuration conf;
  private Map<String, CheckedVolume> volumes;
  private int minimumRedundantVolumes;
  
  @VisibleForTesting
  class CheckedVolume implements CheckableNameNodeResource {
    private DF df;
    private boolean required;
    private String volume;
    
    public CheckedVolume(File dirToCheck, boolean required)
        throws IOException {
      df = new DF(dirToCheck, conf);
      this.required = required;
      volume = df.getFilesystem();
    }
    
    public String getVolume() {
      return volume;
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }

    @Override
    public boolean isResourceAvailable() {
      //获取当前目录空间的大小
      long availableSpace = df.getAvailable();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Space available on volume '" + volume + "' is "
            + availableSpace);
      }
      //如果当前目录空间大小小于100M。则进入安全模式
      if (availableSpace < duReserved) {
        LOG.warn("Space available on volume '" + volume + "' is "
            + availableSpace +
            ", which is below the configured reserved amount " + duReserved);
        return false;
      } else {
        return true;
      }
    }
    
    @Override
    public String toString() {
      return "volume: " + volume + " required: " + required +
          " resource available: " + isResourceAvailable();
    }
  }

  /**
   * Create a NameNodeResourceChecker, which will check the edits dirs and any
   * additional dirs to check set in <code>conf</code>.
   *
   * 这个构造方法主要是：
   * 1、声明namenode容忍的磁盘大小的阈值
   * 2、封装好需要检查的磁盘路径（fsimage和edits）
   * 3、将需要检查的磁盘路径通过addDirToCheck方法添加到volumes这个map集合里面，
   *  然后在FSNameSystem中有一个NameNodeResourceMonitor线程，不断的调用checkAvailableResources方法
   *  来检查volumes（磁盘的资源情况）
   */
  public NameNodeResourceChecker(Configuration conf) throws IOException {
    this.conf = conf;
    //初始化volumes Map，用来存放需要进行磁盘空间检查的路径
    volumes = new HashMap<String, CheckedVolume>();
    //100M
    duReserved = conf.getLong(DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_KEY,
            DFSConfigKeys.DFS_NAMENODE_DU_RESERVED_DEFAULT);
    //除了本地的edits外，本地的其他目录，其实就是只fsimage
    Collection<URI> extraCheckedVolumes = Util.stringCollectionAsURIs(conf
            .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_KEY));
    //本地localEditDirs
    Collection<URI> localEditDirs = Collections2.filter(
            //拿到edits的目录
            FSNamesystem.getNamespaceEditsDirs(conf),
            new Predicate<URI>() {
              @Override
              public boolean apply(URI input) {
                if (input.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
                  return true;
                }
                return false;
              }
            });

    // Add all the local edits dirs, marking some as required if they are
    // configured as such.
    for (URI editsDirToCheck : localEditDirs) {
      //主要是用来存放需要进行磁盘空间检查的dirs
      addDirToCheck(editsDirToCheck,
              FSNamesystem.getRequiredNamespaceEditsDirs(conf).contains(
                      editsDirToCheck));
    }

    // All extra checked volumes are marked "required"
    for (URI extraDirToCheck : extraCheckedVolumes) {
      addDirToCheck(extraDirToCheck, true);
    }

    minimumRedundantVolumes = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
  }

  /**
   * Add the volume of the passed-in directory to the list of volumes to check.
   * If <code>required</code> is true, and this volume is already present, but
   * is marked redundant, it will be marked required. If the volume is already
   * present but marked required then this method is a no-op.
   * 
   * @param directoryToCheck
   *          The directory whose volume will be checked for available space.
   */
  private void addDirToCheck(URI directoryToCheck, boolean required)
      throws IOException {
    File dir = new File(directoryToCheck.getPath());
    if (!dir.exists()) {
      throw new IOException("Missing directory "+dir.getAbsolutePath());
    }
    
    CheckedVolume newVolume = new CheckedVolume(dir, required);
    CheckedVolume volume = volumes.get(newVolume.getVolume());
    if (volume == null || !volume.isRequired()) {
      //TODO
      volumes.put(newVolume.getVolume(), newVolume);
    }
  }

  /**
   * Return true if disk space is available on at least one of the configured
   * redundant volumes, and all of the configured required volumes.
   * 
   * @return True if the configured amount of disk space is available on at
   *         least one redundant volume and all of the required volumes, false
   *         otherwise.
   */
  //监控NameNode主机上的磁盘还是否可用（空间）
  //此处代码是在class NameNodeResourceMonitor implements Runnable中循环调用
  /**
   * 如果一旦发现有资源不足的情况，会使NameNode进入安全模式。
   * 如果随后返回的状态代表资源大小到达可使用的级别，那么这个线程就使NameNode退出安全模式。
    依照这个注释，去解读run()方法的代码逻辑：在一个while循环里，首先判断资源是否可用，
   如果不可用，日志里就会发出一个警告信息，然后调用enterSafeMode();进入安全模式。
   * */
  public boolean hasAvailableDiskSpace() {
    return NameNodeResourcePolicy.areResourcesAvailable(volumes.values(),
        minimumRedundantVolumes);
  }

  /**
   * Return the set of directories which are low on space.
   * 
   * @return the set of directories whose free space is below the threshold.
   */
  @VisibleForTesting
  Collection<String> getVolumesLowOnSpace() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Going to check the following volumes disk space: " + volumes);
    }
    Collection<String> lowVolumes = new ArrayList<String>();
    for (CheckedVolume volume : volumes.values()) {
      lowVolumes.add(volume.getVolume());
    }
    return lowVolumes;
  }
  
  @VisibleForTesting
  void setVolumes(Map<String, CheckedVolume> volumes) {
    this.volumes = volumes;
  }
  
  @VisibleForTesting
  void setMinimumReduntdantVolumes(int minimumRedundantVolumes) {
    this.minimumRedundantVolumes = minimumRedundantVolumes;
  }
}
