/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.api.listener;

import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.internal.guarantee.GuaranteeService;
import com.dangdang.ddframe.job.util.env.TimeService;
import lombok.Setter;

/**
 * 在分布式作业中只执行一次的监听器.
 * 
 * @author zhangliang
 */
public abstract class AbstractDistributeOnceElasticJobListener implements ElasticJobListener {
    
    private final long startedTimeoutMilliseconds;
    
    private final Object startedWait = new Object();
    
    private final long completedTimeoutMilliseconds;
    
    private final Object completedWait = new Object();

    /** 保证分布式任务全部开始和结束状态的服务，通过将分布式任务中的分片项的任务执行状态注册到zk来判断任务是否开始和结束 */
    @Setter
    private GuaranteeService guaranteeService;
    
    private TimeService timeService = new TimeService();
    
    public AbstractDistributeOnceElasticJobListener(final long startedTimeoutMilliseconds, final long completedTimeoutMilliseconds) {
        if (startedTimeoutMilliseconds <= 0L) {
            this.startedTimeoutMilliseconds = Long.MAX_VALUE;
        } else {
            this.startedTimeoutMilliseconds = startedTimeoutMilliseconds;
        }
        if (completedTimeoutMilliseconds <= 0L) {
            this.completedTimeoutMilliseconds = Long.MAX_VALUE; 
        } else {
            this.completedTimeoutMilliseconds = completedTimeoutMilliseconds;
        }
    }

    /**
     * 作业执行前的执行的方法：真正开始执行用户自定义的作业逻辑前，会调用该方法
     *
     * @param shardingContexts 分片上下文
     */
    @Override
    public final void beforeJobExecuted(final ShardingContexts shardingContexts) {

        // 将所有分片项注册到zk上，即创建 /${jobName}/guarantee/started/${shardingItem} 节点
        guaranteeService.registerStart(shardingContexts.getShardingItemParameters().keySet());

        // 如果所有任务都已经开始执行，则删除所有任务启动信息（即移除zk上的 /${jobName}/guarantee/started 节点）
        if (guaranteeService.isAllStarted()) {
            doBeforeJobExecutedAtLastStarted(shardingContexts);
            guaranteeService.clearAllStartedInfo();
            return;
        }

        // 线程阻塞等待一段时间
        long before = timeService.getCurrentMillis();
        try {
            synchronized (startedWait) {
                startedWait.wait(startedTimeoutMilliseconds);
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }

        if (timeService.getCurrentMillis() - before >= startedTimeoutMilliseconds) {
            guaranteeService.clearAllStartedInfo();
            handleTimeout(startedTimeoutMilliseconds);
        }
    }

    /**
     * 作业执行后的执行的方法：执行完用户自定义的作业逻辑后，会调用该方法
     *
     * @param shardingContexts 分片上下文
     */
    @Override
    public final void afterJobExecuted(final ShardingContexts shardingContexts) {
        guaranteeService.registerComplete(shardingContexts.getShardingItemParameters().keySet());
        if (guaranteeService.isAllCompleted()) {
            doAfterJobExecutedAtLastCompleted(shardingContexts);
            guaranteeService.clearAllCompletedInfo();
            return;
        }
        long before = timeService.getCurrentMillis();
        try {
            synchronized (completedWait) {
                completedWait.wait(completedTimeoutMilliseconds);
            }
        } catch (final InterruptedException ex) {
            Thread.interrupted();
        }
        if (timeService.getCurrentMillis() - before >= completedTimeoutMilliseconds) {
            guaranteeService.clearAllCompletedInfo();
            handleTimeout(completedTimeoutMilliseconds);
        }
    }
    
    private void handleTimeout(final long timeoutMilliseconds) {
        throw new JobSystemException("Job timeout. timeout mills is %s.", timeoutMilliseconds);
    }
    
    /**
     * 分布式环境中最后一个作业执行前的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    public abstract void doBeforeJobExecutedAtLastStarted(ShardingContexts shardingContexts);
    
    /**
     * 分布式环境中最后一个作业执行后的执行的方法.
     *
     * @param shardingContexts 分片上下文
     */
    public abstract void doAfterJobExecutedAtLastCompleted(ShardingContexts shardingContexts);
    
    /**
     * 通知任务开始.
     */
    public void notifyWaitingTaskStart() {
        synchronized (startedWait) {
            startedWait.notifyAll();
        }
    }
    
    /**
     * 通知任务结束.
     */
    public void notifyWaitingTaskComplete() {
        synchronized (completedWait) {
            completedWait.notifyAll();
        }
    }
}
