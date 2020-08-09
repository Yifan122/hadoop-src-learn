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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;

  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;

  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
          Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>();
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
              "null future for key: " + e.getKey());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      });
    }
    return qr;
  }

  private QuorumCall() {
    // Only instantiated from factory method above
  }

  /**
   * Wait for the quorum to achieve a certain number of responses.
   *
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  public synchronized void waitFor(
          int minResponses, int minSuccesses, int maxExceptions,
          int millis, String operationName)
          throws InterruptedException, TimeoutException {
    /**
     * 假设有5台机器：
     * minResponses：必须要等待至少minResponses（3）个journalNode给回应（成功+错误）了才会触发return
     * minSuccesses：必须等到至少minSuccesses（3）个journalNode写入成功
     * maxExceptions：最大允许2个journalNode写入失败
     * */
    //假设当前时间是:20:00:00
    long st = Time.monotonicNow();
    //刷日志时间:20:00:00 + 20*0.3 = 20:00:06
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);
    //超时时间: 20:00:00 + 20s = 20:00:20
    long et = st + millis;
    StopWatch stopWatch = new StopWatch();
    while (true) {
      stopWatch.start();
      checkAssertionErrors();
      //第一种情况：当前回应的最小数 > 0 && 总回应数 >= minResponses(3)
      if (minResponses > 0 && countResponses() >= minResponses) return;
      //第二种情况: 当前最小成功数>0 && 回应总数 >= minSuccesses(3)
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      //第三种情况：最大失败数>= 0 && 失败数 >= 2
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;
      //TODO ##############################
      //FULL GC ---> 60s--->now :  20:00:60

      //TODO ##############################
      //20:00:01
      //20:00:01 + 1s + 5s = 20:00:07
      long now = Time.monotonicNow();
      //20:00:01 > 20:00:06
      //20:00:07 > 20:00:06
      if (now > nextLogTime) {
        //20:00:07 - 20:00:00 = 7
        long waited = now - st;
        String msg = String.format(
                "Waited %s ms (timeout=%s ms) for a response for %s",
                waited, millis, operationName);
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        //7 > 20*0.7=14
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          QuorumJournalManager.LOG.info(msg);
        }
        //20:00:07 + 1 = 20:00:08
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }
      //20:00:20 - 20:00:01 = 19
      //20:00:20 - 20:00:07 = 13
      long rem = et - now;
      Configuration conf = new HdfsConfiguration();
      if (rem <= 0) {
        //TODO ####################
        final long elapse = stopWatch.getElapse();//拿到流逝的时间
        /**
         *  boolean https = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY,
         DFSConfigKeys.DFS_HTTPS_ENABLE_DEFAULT);

         * */
        if(elapse >= conf.getLong(DFSConfigKeys.DFS_NAMENODE_GC_VALUE_KEY , DFSConfigKeys.DFS_NAMENODE_GC_DEFAULT_VALUE)) {
          et = et + elapse;
        }else{
          throw new TimeoutException();
        }
      }
      //TODO ####################
      stopWatch.restart();//TODO ####################
      //min(19 , 20:00:06 - 20:00:01 = 5) = 5
      //min(13 , 20:00:08 - 20:00:07) = 1
      rem = Math.min(rem, nextLogTime - now);
      //max(5 , 1) = 5
      //MAX(1,1) = 1
      rem = Math.max(rem, 1);
      //TODO ##############################
      //FULL GC ---> 60s--->now :  20:00:60

      //TODO ##############################
      wait(rem);
      //TODO ##############################
      //FULL GC ---> 60s--->now :  20:00:60

      //TODO ##############################
      final long elapse = stopWatch.getElapse();
      if(elapse >= conf.getLong(DFSConfigKeys.DFS_NAMENODE_GC_VALUE_KEY , DFSConfigKeys.DFS_NAMENODE_GC_DEFAULT_VALUE)) {
        et = et + elapse - rem ;
      }
    }
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   *
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing through.
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
                ((RemoteException)t).getClassName().equals(
                        AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }

  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }

  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }

  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }

  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
          Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
              .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
              .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }

  //TODO 获取流逝的时间
  static class StopWatch{
    //开始时间
    long start = 0L ;
    //统计时间流逝
    long elapse = 0L ;

    //开始计时
    public StopWatch start(){
      start = System.currentTimeMillis() ;
      return this;
    }

    //重置时间
    public StopWatch reset(){
      start = 0 ;
      elapse = 0;
      return this ;
    }

    //重新计时
    public StopWatch restart(){
      return this.reset().start() ;
    }

    //获取流逝的时间
    public long getElapse(){
      return System.currentTimeMillis() - start ;
    }
  }
}
