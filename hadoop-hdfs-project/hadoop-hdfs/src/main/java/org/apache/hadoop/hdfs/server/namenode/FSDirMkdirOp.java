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

import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.util.Time.now;

class FSDirMkdirOp {


  /**
   * FSDirectory和FSNamesystem类类似 管理命名空间的状态：
   * FSDirectory 完全是内存的数据结构，它的所有操作都发生在内存中。
   * 相反，FSNamesystem把所有的操作都持久化到磁盘上（先写内存，在写磁盘）。
   *
   *
   * 1、校验路径的合法性
   * 2、得到当前创建的路径在元数据中尚未存在的路径
   * 3、循环创建不存在的路径，并将元数据刷到磁盘
   * 4、返回当前hdfs的元数据
   * */
  static HdfsFileStatus mkdirs(FSNamesystem fsn, String src,
      PermissionStatus permissions, boolean createParent) throws IOException {
    //TODO
    FSDirectory fsd = fsn.getFSDirectory();
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    }
    //1、校验是否是有效路径（以/为开头）
    if (!DFSUtil.isValidName(src)) {
      throw new InvalidPathException(src);
    }
    FSPermissionChecker pc = fsd.getPermissionChecker();
    //得到二进制文件名称（如果路径不以/.reserved/（源文件）为开头则返回null）
    byte[][] pathComponents = FSDirectory.getPathComponentsForReservedPath(src);
    fsd.writeLock();
    try {
      //2、得到处理后的src路径：/usr/hive/warehose/data
      src = fsd.resolvePath(pc, src, pathComponents);
      //3、将当前路径解析成INode结构路径
      INodesInPath iip = fsd.getINodesInPath4Write(src);
      if (fsd.isPermissionEnabled()) {//检查权限
        fsd.checkTraverse(pc, iip);
      }
      /**
       * 4、
       * 获取上一个INode，eg：
       * 现有路径：/user/hive/warehouse
       * 需要创建：/user/hive/warehouse/data
       * 上一个INode就是：warehouse
       * 如果之前的节点是不存在的（/user/hive/warehouse），那么lastINode=/
       * */
      final INode lastINode = iip.getLastINode();
      if (lastINode != null && lastINode.isFile()) {
        throw new FileAlreadyExistsException("Path is not a directory: " + src);
      }

      //如果上一个节点不为null，则返回当前传入的INode结构 ， 否则返回现有存在的结构路径
      INodesInPath existing = lastINode != null ? iip : iip.getExistingINodes();
      //权限校验，如果上一个节点是null，那么就校验一下当前传入的节点Inode，后续开始针对INode处理
      if (lastINode == null) {
        if (fsd.isPermissionEnabled()) {
          fsd.checkAncestorAccess(pc, iip, FsAction.WRITE);
        }

        if (!createParent) {
          fsd.verifyParentDir(iip, src);
        }
        fsn.checkFsObjectLimit();
        /**
         * 如果创建的路径是/usr/local/warehose
         * 那么返回的list就是：List[0]  = usr , List[1]=local , List[2]=warehose ,
         * 如果/usr/local都是存在的，那么list[0]=warehose,既nonExisting里面封装的是不存在的节点路径
         * */
        List<String> nonExisting = iip.getPath(existing.length(),
            iip.length() - existing.length());
        int length = nonExisting.size();
        /**
         * createChildrenDirectories：
         * 1、根据传入的待添加的节点名称，通过内存中的FSDirectory，得到节点树结构
         * 2、获取新添加的节点INode和添加后的路径，然后将元数据信息刷到磁盘
         * */
        //如果返回的nonExisting是多级的，则length>1，需要按层创建（顺序）
        if (length > 1) {
          List<String> ancestors = nonExisting.subList(0, length - 1);//从待添加路径开始
          //TODO
          existing = createChildrenDirectories(fsd, existing, ancestors, addImplicitUwx(permissions, permissions));
          if (existing == null) {
            throw new IOException("Failed to create directory: " + src);
          }
        }
        //只创建一个目录，则走这里
        if ((existing = createChildrenDirectories(fsd, existing, nonExisting.subList(length - 1, length), permissions)) == null) {
          throw new IOException("Failed to create directory: " + src);
        }
      }
      return fsd.getAuditFileInfo(existing);
    } finally {
      fsd.writeUnlock();
    }
  }

  /**
   * For a given absolute path, create all ancestors as directories along the
   * path. All ancestors inherit their parent's permission plus an implicit
   * u+wx permission. This is used by create() and addSymlink() for
   * implicitly creating all directories along the path.
   *
   * For example, path="/foo/bar/spam", "/foo" is an existing directory,
   * "/foo/bar" is not existing yet, the function will create directory bar.
   *
   * @return a tuple which contains both the new INodesInPath (with all the
   * existing and newly created directories) and the last component in the
   * relative path. Or return null if there are errors.
   */
  static Map.Entry<INodesInPath, String> createAncestorDirectories(
      FSDirectory fsd, INodesInPath iip, PermissionStatus permission)
      throws IOException {
    final String last = new String(iip.getLastLocalName(), Charsets.UTF_8);//获取最后一个节点名称
    INodesInPath existing = iip.getExistingINodes();//获取要创建的节点路径(不包含最后一个节点)
    List<String> children = iip.getPath(existing.length(),//拿到要创建的节点，封装到list里面，最为要添加的子节点
        iip.length() - existing.length());
    int size = children.size();//拿到要创建的子节点个数
    if (size > 1) { // 如果要创建的子节点个数大于1 ：那么通过循环的方式创建进去 ， 否则last和existing就满足了要创建的内容
      List<String> directories = children.subList(0, size - 1);
      INode parentINode = existing.getLastINode();
      // Ensure that the user can traversal the path by adding implicit
      // u+wx permission to all ancestor directories
      existing = createChildrenDirectories(fsd, existing, directories,
          addImplicitUwx(parentINode.getPermissionStatus(), permission));
      if (existing == null) {
        return null;
      }
    }
    return new AbstractMap.SimpleImmutableEntry<>(existing, last);
  }

  /**
   * Create the directory {@code parent} / {@code children} and all ancestors
   * along the path.
   *
   * @param fsd FSDirectory
   * @param existing 已经存在的路径
   * @param children 待添加的路径
   * @param perm the permission of the directory. Note that all ancestors
   *             created along the path has implicit {@code u+wx} permissions.
   *
   * @return {@link INodesInPath} which contains all inodes to the
   * target directory, After the execution parentPath points to the path of
   * the returned INodesInPath. The function return null if the operation has
   * failed.
   */
  private static INodesInPath createChildrenDirectories(FSDirectory fsd,
      INodesInPath existing, List<String> children, PermissionStatus perm)
      throws IOException {
    assert fsd.hasWriteLock();
    //循环List里面的目录，一个一个创建
    for (String component : children) {
      existing = createSingleDirectory(fsd, existing, component, perm);
      if (existing == null) {
        return null;
      }
    }
    return existing;
  }

    static void mkdirForEditLog(FSDirectory fsd, long inodeId, String src,
      PermissionStatus permissions, List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, UnresolvedLinkException, AclException,
      FileAlreadyExistsException {
    assert fsd.hasWriteLock();
    //获取要新增的路径
    INodesInPath iip = fsd.getINodesInPath(src, false);
    //最新的节点名称
    final byte[] localName = iip.getLastLocalName();
    //获取已存在的父级目录
    final INodesInPath existing = iip.getParentINodesInPath();
    Preconditions.checkState(existing.getLastINode() != null);
    unprotectedMkdir(fsd, inodeId, existing, localName, permissions, aclEntries,
        timestamp);
  }
  /**
   * 1、根据传入的待添加节点，更新文件目录树，这颗目录树是存在于内存中，由FSNameSystem管理
   * 2、获取新添加的节点INode和添加后的路径，然后将元数据信息刷到磁盘
   * */
  private static INodesInPath createSingleDirectory(FSDirectory fsd,
      INodesInPath existing, String localName, PermissionStatus perm)
      throws IOException {
    assert fsd.hasWriteLock();
    //TODO 更新文件目录树，这颗目录树是存在于内存中，由FSDirectory管理
    existing = unprotectedMkdir(fsd, fsd.allocateNewInodeId(), existing,
        localName.getBytes(Charsets.UTF_8), perm, null, now());
    if (existing == null) {
      return null;
    }
    //获取新构建的节点
    final INode newNode = existing.getLastINode();
    // Directory creation also count towards FilesCreated
    // to match count of FilesDeleted metric.
    NameNode.getNameNodeMetrics().incrFilesCreated();
    //获取当前路径（已经存在的新节点）,比如新添加节点是/test , 之前的节点是/usr ,那么cur就是：/usr/test
    String cur = existing.getPath();
    //TODO 把元数据信息记录到磁盘上（先写内存，在写磁盘）(高并发多线程)
    fsd.getEditLog().logMkDir(cur, newNode);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("mkdirs: created directory " + cur);
    }
    return existing;
  }

  private static PermissionStatus addImplicitUwx(PermissionStatus parentPerm,
      PermissionStatus perm) {
    FsPermission p = parentPerm.getPermission();
    FsPermission ancestorPerm = new FsPermission(
        p.getUserAction().or(FsAction.WRITE_EXECUTE),
        p.getGroupAction(),
        p.getOtherAction());
    return new PermissionStatus(perm.getUserName(), perm.getGroupName(),
        ancestorPerm);
  }

  /**
   * create a directory at path specified by parent
   *
   * parent是当前已经存在的父目录
   * permission文件权限
   */
  private static INodesInPath unprotectedMkdir(FSDirectory fsd, long inodeId,
      INodesInPath parent, byte[] name, PermissionStatus permission,
      List<AclEntry> aclEntries, long timestamp)
      throws QuotaExceededException, AclException, FileAlreadyExistsException {
    assert fsd.hasWriteLock();
    assert parent.getLastINode() != null;
    if (!parent.getLastINode().isDirectory()) {//如果已经存在的父目录不存在，则抛异常终止执行
      throw new FileAlreadyExistsException("Parent path is not a directory: " +
          parent.getPath() + " " + DFSUtil.bytes2String(name));
    }
    /**
     * FSDirectory文件目录树，代表/目录
     * INodeDirectory代表目录
     * INodeFile代表文件
     *
     * */
    //如果是多重目录，则构建第一个不存在的节点
    final INodeDirectory dir = new INodeDirectory(inodeId, name, permission,
        timestamp);
    //将上面构建的节点添加到父目录下面，构成树结构 TODO
    INodesInPath iip = fsd.addLastINode(parent, dir, true);
    if (iip != null && aclEntries != null) {//
      AclStorage.updateINodeAcl(dir, aclEntries, Snapshot.CURRENT_STATE_ID);
    }
    return iip;
  }
}

