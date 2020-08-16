# HDFS src learning 

## Fully commented code for HDFS

## 源代码的改进

- 双缓冲区高并发时缓冲大小不足，造成大量wait
    - 在Editlog.java 中有个判断是否进行刷盘
```java
void logEdit(final FSEditLogOp op) {
    synchronized (this) {
      assert isOpenForWrite() :
              "bad state: " + state;

      // 如果当前操作被其他线程调度了，则等待1s时间（同步操作）
      waitIfAutoSyncScheduled();
      /**
       * name: editlog_0000000_00000022.log
       *       editlog_0000023_00000055.log
       * */
      //TODO 步骤1：获取当前独一无二的事务ID
      long start = beginTransaction();
      //设置事物ID
      op.setTransactionId(txid);
      try {
        /**
         * 将数据刷到JournalNode
         * editLogStream包括2个流：
         * 1）：写namenode
         * 2）：写JournalNode
         * */
        //TODO 步骤2：将元数据信息写入内存(其实内部就是往bufCurrent写数据)
        editLogStream.write(op);

      } catch (IOException ex) {
        // All journals failed, it is handled in logSync.
      } finally {
        op.reset();
      }
      //设置结束时间，然后把执行时间：end-start结果放入metrics
      endTransaction(start);
      //TODO 不满足需求，那么立马return
      if (!shouldForceSync()) {//!false
        return;
      }
      //true，标识bufCurrent满了，进行双缓冲刷盘了
      isAutoSyncScheduled = true;
    }
    //TODO 步骤3 将缓冲的编辑日志条目同步到持久性存储
    logSync();
  }
```
其中shouldForceSync() 的实现是
```java
public boolean shouldForceSync() {
    //TODO 如果当前缓冲区内大小 > 512k
    return bufCurrent.size() >= initBufferSize;
 }
```
initBufferSize是写死的，在FileJournalManager中为512K，需要将其改成可配置的。

代码修改如下：
```java
// in FileJournalManager.java
// DFS_NAMENODE_BUFFER_VALUE_KEY = dfs.namenode.buffer.value
// DFS_NAMENODE_BUFFER_DEFAULT_VALUE = 10 * 512 * 1024 //5M
this.outputBufferCapacity = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_BUFFER_VALUE_KEY ,
            DFSConfigKeys.DFS_NAMENODE_BUFFER_DEFAULT_VALUE);

```
in hdfs-default.xml 修改如下
```xml
  <property>
    <name>dfs.namenode.buffer.value</name>
    <value>5242880</value>
  </property>
```

