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

package com.dangdang.ddframe.job.lite.internal.failover;

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingNode;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.lite.internal.storage.LeaderExecutionCallback;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 作业失效转移服务.
 * 
 * @author zhangliang
 */
@Slf4j
public final class FailoverService {

    /** 作业名称 */
    private final String jobName;

    /** 用于操作zk上的节点，例如：增删改查等 */
    private final JobNodeStorage jobNodeStorage;

    /** 作业分片服务 */
    private final ShardingService shardingService;
    
    public FailoverService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }
    
    /**
     * 设置失效的分片项标记.
     * 
     * @param item 崩溃的作业项
     */
    public void setCrashedFailoverFlag(final int item) {
        if (!isFailoverAssigned(item)) {
            jobNodeStorage.createJobNodeIfNeeded(FailoverNode.getItemsNode(item));
        }
    }
    
    private boolean isFailoverAssigned(final Integer item) {
        return jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(item));
    }
    
    /**
     * 如果需要失效转移, 则执行作业失效转移.
     */
    public void failoverIfNecessary() {
        if (needFailover()) {
            jobNodeStorage.executeInLeader(FailoverNode.LATCH, new FailoverLeaderExecutionCallback());
        }
    }

    /**
     * 查看zk上是否存在有需要失效转移的作业
     *
     * @return
     */
    private boolean needFailover() {
        return jobNodeStorage.isJobNodeExisted(FailoverNode.ITEMS_ROOT) && !jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).isEmpty()
                && !JobRegistry.getInstance().isJobRunning(jobName);
    }
    
    /**
     * 更新执行完毕失效转移的分片项状态.
     * 
     * @param items 执行完毕失效转移的分片项集合
     */
    public void updateFailoverComplete(final Collection<Integer> items) {
        for (int each : items) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(each));
        }
    }
    
    /**
     * 获取作业服务器的失效转移分片项集合.
     *
     * @param jobInstanceId 作业运行实例主键
     * @return 作业失效转移的分片项集合
     */
    public List<Integer> getFailoverItems(final String jobInstanceId) {
        List<String> items = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT);
        List<Integer> result = new ArrayList<>(items.size());
        for (String each : items) {
            int item = Integer.parseInt(each);
            String node = FailoverNode.getExecutionFailoverNode(item);
            if (jobNodeStorage.isJobNodeExisted(node) && jobInstanceId.equals(jobNodeStorage.getJobNodeDataDirectly(node))) {
                result.add(item);
            }
        }
        Collections.sort(result);
        return result;
    }
    
    /**
     * 获取运行在本作业服务器的失效转移分片项集合.
     * 
     * @return 运行在本作业服务器的失效转移分片项集合
     */
    public List<Integer> getLocalFailoverItems() {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return Collections.emptyList();
        }
        return getFailoverItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * 获取运行在本作业服务器的被失效转移的序列号.
     * 
     * @return 运行在本作业服务器的被失效转移的序列号
     */
    public List<Integer> getLocalTakeOffItems() {
        List<Integer> shardingItems = shardingService.getLocalShardingItems();
        List<Integer> result = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (jobNodeStorage.isJobNodeExisted(FailoverNode.getExecutionFailoverNode(each))) {
                result.add(each);
            }
        }
        return result;
    }
    
    /**
     * 删除作业失效转移信息.
     */
    public void removeFailoverInfo() {
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT)) {
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getExecutionFailoverNode(Integer.parseInt(each)));
        }
    }

    /**
     * 作业失效转移的回调服务
     */
    class FailoverLeaderExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            // 如果zk不存在需要失效转移的作业，则结束回调方法
            if (JobRegistry.getInstance().isShutdown(jobName) || !needFailover()) {
                return;
            }

            // 获取第一个需要转移的分片项
            int crashedItem = Integer.parseInt(jobNodeStorage.getJobNodeChildrenKeys(FailoverNode.ITEMS_ROOT).get(0));
            log.debug("Failover job '{}' begin, crashed item '{}'", jobName, crashedItem);

            // 设置 /${jobName}/sharding/${item}/failover 节点数据为作业实例ID，例如：172.16.120.135@-@97544
            jobNodeStorage.fillEphemeralJobNode(FailoverNode.getExecutionFailoverNode(crashedItem), JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
            // 移除 /${jobName}/leader/failover/items/${item} 节点
            jobNodeStorage.removeJobNodeIfExisted(FailoverNode.getItemsNode(crashedItem));
            // TODO 不应使用triggerJob, 而是使用executor统一调度
            JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);
            if (null != jobScheduleController) {
                // 触发任务
                jobScheduleController.triggerJob();
            }
        }
    }
}
