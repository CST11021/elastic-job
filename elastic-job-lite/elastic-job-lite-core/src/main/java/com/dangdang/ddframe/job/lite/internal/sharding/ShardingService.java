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

package com.dangdang.ddframe.job.lite.internal.sharding;

import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.api.strategy.JobShardingStrategy;
import com.dangdang.ddframe.job.lite.api.strategy.JobShardingStrategyFactory;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.election.LeaderService;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.lite.internal.storage.TransactionExecutionCallback;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.concurrent.BlockUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 作业分片服务：
 *
 * 1、三种情况会触发分片：
 *  1.1、当有新的作业实例启动的时候；
 *  1.2、摸个作业对应的作业实例列表发生变更的时候；
 *  1.3、作业分片总数发生变更的时候
 *  触发分片时，会给在zk上添加${jobName}/leader/sharding/necessary节点，表该作业需要分片
 * 
 * @author zhangliang
 */
@Slf4j
public final class ShardingService {
    /** 作业名称 */
    private final String jobName;
    /** 作业节点数据访问类 */
    private final JobNodeStorage jobNodeStorage;
    
    private final LeaderService leaderService;
    
    private final ConfigurationService configService;
    
    private final InstanceService instanceService;
    
    private final ServerService serverService;
    
    private final ExecutionService executionService;

    private final JobNodePath jobNodePath;



    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }




    /**
     * 设置需要重新分片的标记，如果存在${jobName}/leader/sharding/necessary节点，表示需要重新分片
     */
    public void setReshardingFlag() {
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY);
    }
    
    /**
     * 判断是否需要重分片，如果在${jobName}/leader/sharding/necessary节点，表示需要重新分片
     * 
     * @return 是否需要重分片
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY);
    }
    
    /**
     * 如果需要分片，并且当前节点为主节点, 则作业分片.
     * 
     * <p>
     * 如果当前无可用节点则不分片.
     * </p>
     */
    public void shardingIfNecessary() {

        // 如果zk不存在分片标识，或者可用的作业实例列表是空的，则不需要分片
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances();
        if (!isNeedSharding() || availableJobInstances.isEmpty()) {
            return;
        }

        // 如果该实例不是leader，则阻塞线程，直到主节点被选举出来并且分片完成为止
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilShardingCompleted();
            return;
        }



        // 执行到这里说明当前实例是leader，则进行分片


        // 等待所有分片任务执行完成，当/sharding/${item}/节点下不存在running节点了，说明当前已经没有实例在执行该作业了
        waitingOtherJobCompleted();

        // 获取分片总数
        LiteJobConfiguration liteJobConfig = configService.load(false);
        int shardingTotalCount = liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount();
        log.debug("Job '{}' sharding begin.", jobName);

        // 创建临时节点：leader/sharding/processing
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, "");

        // 重置sharding/节点的下的分片数量，创建对应数量的分片节点
        resetShardingInfo(shardingTotalCount);

        // 根据分片策略，给每个实例进行分片
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(liteJobConfig.getJobShardingStrategyClass());
        Map<JobInstance, List<Integer>> map = jobShardingStrategy.sharding(availableJobInstances, jobName, shardingTotalCount);

        // 将分片结果保存到zk，并创建相应的节点
        jobNodeStorage.executeInTransaction(new PersistShardingInfoTransactionExecutionCallback(map));
        log.debug("Job '{}' sharding complete.", jobName);
    }
    
    private void blockUntilShardingCompleted() {
        while (!leaderService.isLeaderUntilBlock() && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY) || jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }

    /**
     * 等待所有分片任务执行完成
     */
    private void waitingOtherJobCompleted() {
        while (executionService.hasRunningItems()) {
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }

    /**
     * 重置sharding/节点的下的分片数量
     *
     * @param shardingTotalCount
     */
    private void resetShardingInfo(final int shardingTotalCount) {
        for (int i = 0; i < shardingTotalCount; i++) {
            // 删除 sharding/%s/instance 节点
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i));
            // 创建 sharding/%s 节点
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i);
        }

        // 获取当前 /sharding 下的分片数量，将多出来的分片删除
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size();
        if (actualShardingTotalCount > shardingTotalCount) {
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i);
            }
        }
    }
    
    /**
     * 获取作业运行实例的分片项集合，一个作业实例可以分配到多个分片
     *
     * @param jobInstanceId 作业运行实例主键
     * @return 作业运行实例的分片项集合
     */
    public List<Integer> getShardingItems(final String jobInstanceId) {
        JobInstance jobInstance = new JobInstance(jobInstanceId);
        if (!serverService.isAvailableServer(jobInstance.getIp())) {
            return Collections.emptyList();
        }

        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            // sharding/%s/instance
            String data = jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i));
            if (jobInstance.getJobInstanceId().equals(data)) {
                result.add(i);
            }
        }
        return result;
    }
    
    /**
     * 获取运行在本作业实例的分片项集合.
     * 
     * @return 运行在本作业实例的分片项集合
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
            return Collections.emptyList();
        }

        return getShardingItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * 查询是包含有分片节点的不在线服务器.
     * 
     * @return 是包含有分片节点的不在线服务器
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT);
        int shardingTotalCount = configService.load(true).getTypeConfig().getCoreConfig().getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }


    @RequiredArgsConstructor
    class PersistShardingInfoTransactionExecutionCallback implements TransactionExecutionCallback {

        /** 表示分片结果 */
        private final Map<JobInstance, List<Integer>> shardingResults;

        /**
         * 当分片完成后，会调用该方法
         *
         * @param curatorTransactionFinal 执行事务的上下文
         * @throws Exception
         */
        @Override
        public void execute(final CuratorTransactionFinal curatorTransactionFinal) throws Exception {
            // 遍历每个分片，创建对应的sharding/%s/instance节点
            for (Map.Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
                for (int shardingItem : entry.getValue()) {
                    // sharding/%s/instance
                    String instancePath = jobNodePath.getFullPath(ShardingNode.getInstanceNode(shardingItem));
                    String jobInstanceId = entry.getKey().getJobInstanceId();
                    curatorTransactionFinal.create().forPath(instancePath, jobInstanceId.getBytes()).and();
                }
            }

            // 删除leader/sharding/necessary节点
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.NECESSARY)).and();
            // leader/sharding/processing节点
            curatorTransactionFinal.delete().forPath(jobNodePath.getFullPath(ShardingNode.PROCESSING)).and();
        }
    }
}
