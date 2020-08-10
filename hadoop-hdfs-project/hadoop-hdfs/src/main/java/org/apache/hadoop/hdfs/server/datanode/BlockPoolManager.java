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
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * 为DataNode节点管理BPOfferService对象。
 * 对于BPOfferService对象的创建、移除、启动、停止等操作必须通过该类的api来完成。
 */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Log LOG = DataNode.LOG;
  //按 NameserviceId 查找（nameserviceId我们可以理解为HDFS集群中某一特定命名服务空间的唯一标识）
  private final Map<String, BPOfferService> bpByNameserviceId = Maps.newHashMap();
  //按 BlockPoolId 查找（命名服务空间中的一个块池，或者说一组数据块的唯一标识）
  private final Map<String, BPOfferService> bpByBlockPoolId = Maps.newHashMap();
  //对于每个 Nameservice 都有个联络组
  private final List<BPOfferService> offerServices = Lists.newArrayList();

  private final DataNode dn;

  //refreshNamenodes()方法中用于线程间同步或互斥锁的Object：refreshNamenodesLock
  private final Object refreshNamenodesLock = new Object();
  
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions.checkArgument(offerServices.contains(bpos),
        "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns the array of BPOfferService objects. 
   * Caution: The BPOfferService returned could be shutdown any time.
   */
  synchronized BPOfferService[] getAllNamenodeThreads() {
    BPOfferService[] bposArray = new BPOfferService[offerServices.size()];
    return offerServices.toArray(bposArray);
  }
      
  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  synchronized void remove(BPOfferService t) {
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // It's possible that the block pool never successfully registered
      // with any NN, so it was never added it to this map
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed;) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  void shutDownAll(BPOfferService[] bposArray) throws InterruptedException {
    if (bposArray != null) {
      for (BPOfferService bpos : bposArray) {
        bpos.stop(); //interrupts the threads
      }
      //now join
      for (BPOfferService bpos : bposArray) {
        bpos.join();
      }
    }
  }
  /**遍历List<BPOfferService> ， 逐个启动*/
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              //逐个调用BPOfferService#start()，启动BPOfferService
              for (BPOfferService bpos : offerServices) {
                bpos.start();
              }
              return null;
            }
          });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  void joinAll() {
    for (BPOfferService bpos: this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }
  
  void refreshNamenodes(Configuration conf)
      throws IOException {
    LOG.info("Refresh request received for nameservices: " + conf.get
            (DFSConfigKeys.DFS_NAMESERVICES));

    // 从配置信息conf中获取nameserviceid->{namenodeid->InetSocketAddress}的映射集合newAddressMap
    Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil
            .getNNServiceRpcAddressesForCluster(conf);

    synchronized (refreshNamenodesLock) {
      //newAddressMap是从配置文件中获取到的
      doRefreshNamenodes(newAddressMap);
    }
  }
  /**
   * 本方法分成5个步骤来完成对命名空间的新增、刷新、删除、以及对新增命名空间添加BPOfferService线程，并启动所有线程
   * 1. datanode的握手
   * 2. datanode的心跳
   * 3. datanode的注册
   * 4. datanode的数据块汇报
   * */
  private void doRefreshNamenodes(
      Map<String, Map<String, InetSocketAddress>> addrMap) throws IOException {
    // 确保当前线程在refreshNamenodesLock上拥有互斥锁
    assert Thread.holdsLock(refreshNamenodesLock);
    // 定义三个集合，分别为待刷新的toRefresh、待添加的toAdd和待移除的toRemove
    Set<String> toRefresh = Sets.newLinkedHashSet();
    Set<String> toAdd = Sets.newLinkedHashSet();
    Set<String> toRemove;
    
    synchronized (this) {
      // Step 1. For each of the new nameservices, figure out whether
      // it's an update of the set of NNs for an existing NS,
      // or an entirely new nameservice.
      /**
       * nameservice（命名空间）：指的就是目录
       * ##################################################################
       * HA状态下的hadoop，有两个namenode：1、ActiveNameNode 2、StandByNameNode
       * 这两个namenode管理的元数据是一样的，这两个管理的是同一个nameservice
       * 但是，在联邦模式下会有多套HA架构，就会有多个nameservice：
       * 比如：
       * namenode：
       * （hadoop01 ， hadoop02）共同维护一个nameservice
       * （hadoop03 ， hadoop04）共同维护一个nameservice
       * ##################################################################
       * */
      //遍历addrMap，有多少套联邦，就会有多少nameservice，并添加到toAdd
      /**
       第一步，针对nameserviceid->{namenode名称->InetSocketAddress}的映射集合newAddressMap中每个nameserviceid，
       确认它是一个完全新加的nameservice，还是一个其NameNode列表被更新的nameservice，
       分别加入待添加toAdd和待刷新toRefresh集合；
       * */
      for (String nameserviceId : addrMap.keySet()) {
        // 如果bpByNameserviceId结合中存在nameserviceId，加入待刷新集合toRefresh，否则加入到待添加集合toAdd
        if (bpByNameserviceId.containsKey(nameserviceId)) {
          toRefresh.add(nameserviceId);
        } else {
          toAdd.add(nameserviceId);
        }
      }
      
      // Step 2. Any nameservices we currently have but are no longer present
      // need to be removed.
      //第二步，针对newAddressMap中没有，而目前DataNode内存bpByNameserviceId中存在的nameservice，
      // 需要删除，添加到待删除toRemove集合；

      // 加入到待删除集合toRemove
      toRemove = Sets.newHashSet(Sets.difference(
          bpByNameserviceId.keySet(), addrMap.keySet()));

      // 验证，待刷新集合toRefresh的大小与待添加集合toAdd的大小必须等于配置信息addrMap中的大小
      assert toRefresh.size() + toAdd.size() ==
        addrMap.size() :
          "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
          "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
          "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

      
      // Step 3. Start new nameservices
      // 第三步，启动所有新的nameservices
      /**
       * 为每个namespace创建对应的BPOfferService（包括每个namenode对应的BPServiceActor），
       * 然后通过BlockPoolManager#startAll()启动所有BPOfferService（实际是启动所有BPServiceActor）。
       * */
      if (!toAdd.isEmpty()) {// 待添加集合toAdd不为空
        LOG.info("Starting BPOfferServices for nameservices: " + Joiner.on(",").useForNull("<default>").join(toAdd));

        //针对待添加集合toAdd中的每个nameserviceId，做以下处理：
        for (String nsToAdd : toAdd) {
          // 从addrMap中根据nameserviceId获取对应Socket地址InetSocketAddress，创建集合addrs
          ArrayList<InetSocketAddress> addrs = Lists.newArrayList(addrMap.get(nsToAdd).values());
          // 创建BPOfferService
          BPOfferService bpos = createBPOS(addrs);
          // 将nameserviceId->BPOfferService的对应关系添加到集合bpByNameserviceId中
          bpByNameserviceId.put(nsToAdd, bpos);
          // 将BPOfferService添加到集合offerServices中
          offerServices.add(bpos);
        }
      }
      // TODO 启动所有offerServices的BPOfferService
      startAll();
    }


    // 第4步，停止所有旧的nameservices。这个是发生在synchronized代码块外面的，是因为它们需要回调另外一个线程的remove()方法
    if (!toRemove.isEmpty()) {
      LOG.info("Stopping BPOfferServices for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRemove));
      // 遍历待删除集合toRemove中的每个nameserviceId
      for (String nsToRemove : toRemove) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
        bpos.stop();
        bpos.join();
        // they will call remove on their own
      }
    }
    
    // Step 5. Update nameservices whose NN list has changed
    // 第5步，更新NN列表已变化的nameservices
    if (!toRefresh.isEmpty()) {
      LOG.info("Refreshing list of NNs for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRefresh));
      
      for (String nsToRefresh : toRefresh) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
        // 根据BPOfferService从配置信息addrMap中取出NN的Socket地址InetSocketAddress，形成列表addrs
        ArrayList<InetSocketAddress> addrs = Lists.newArrayList(addrMap.get(nsToRefresh).values());
        // 调用BPOfferService的refreshNNList()方法根据addrs刷新NN列表
        bpos.refreshNNList(addrs);
      }
    }
  }

  /**
   * Extracted out for test purposes.
   * BPOfferService通过bpServices维护同一个namespace下各namenode对应的BPServiceActor。
   */
  protected BPOfferService createBPOS(List<InetSocketAddress> nnAddrs) {
    return new BPOfferService(nnAddrs, dn);
  }
}
