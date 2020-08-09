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

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Given a set of checkable resources, this class is capable of determining
 * whether sufficient resources are available for the NN to continue operating.
 */
@InterfaceAudience.Private
final class NameNodeResourcePolicy {

  /**
   * Return true if and only if there are sufficient NN
   * resources to continue logging edits.
   * 
   * @param resources the collection of resources to check.
   * @param minimumRedundantResources the minimum number of redundant resources
   *        required to continue operation.
   * @return true if and only if there are sufficient NN resources to
   *         continue logging edits.
   */

  /**
   * 主要对volumns里面的url进行检查,看看这些url路径是否可用，是否满足继续运行的最小资源数
   * */
  static boolean areResourcesAvailable(
      Collection<? extends CheckableNameNodeResource> resources,
      int minimumRedundantResources) {

    // TODO: workaround:
    // - during startup, if there are no edits dirs on disk, then there is
    // a call to areResourcesAvailable() with no dirs at all, which was
    // previously causing the NN to enter safemode
    //如果resources为null，则说明没有本地的edits目录，那么可能是刚启动或者刚格式化
    if (resources.isEmpty()) {
      return true;
    }
    //需要的数量
    int requiredResourceCount = 0;
    //冗余的数量
    int redundantResourceCount = 0;
    //无法使用的冗余资源数
    int disabledRedundantResourceCount = 0;
    for (CheckableNameNodeResource resource : resources) {
      //如果不是当前namenode需要的资源（edits路径），则redundantResourceCount++;
      if (!resource.isRequired()) {
        redundantResourceCount++;
        //如果目录不可用，则disabledRedundantResourceCount++;
        if (!resource.isResourceAvailable()) {//isResourceAvailable --》检查目录空间大小
          disabledRedundantResourceCount++;
        }
      } else {//如果当前的路径是namenode需要的，并且空间不够100M，那么返回false，直接进入安全模式
        requiredResourceCount++;
        if (!resource.isResourceAvailable()) {
          // Short circuit - a required resource is not available.
          return false;
        }
      }
    }
    
    if (redundantResourceCount == 0) {
      // If there are no redundant resources, return true if there are any
      // required resources available.
      return requiredResourceCount > 0;
    } else {
      //minimumRedundantResources 继续运行所需要的最少冗余资源数
      //冗余的数量 - 无法使用的冗余资源数 >= 继续运行所需要的最少冗余资源数
      return redundantResourceCount - disabledRedundantResourceCount >=
          minimumRedundantResources;
    }
  }
}
