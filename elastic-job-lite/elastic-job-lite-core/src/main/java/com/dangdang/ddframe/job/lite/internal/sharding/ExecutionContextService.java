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

import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.config.ShardingItemParameters;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作业运行时上下文服务，用于获取运行时的分片上下文
 * 
 * @author zhangliang
 */
public final class ExecutionContextService {

    /** 作业名称 */
    private final String jobName;
    /** 用于操作zk上的节点，例如：增删改查等 */
    private final JobNodeStorage jobNodeStorage;
    /** 用于从注册中心读取 LiteJobConfiguration 配置，和注册作业配置到注册中心 */
    private final ConfigurationService configService;
    
    public ExecutionContextService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
    }
    
    /**
     * 获取当前作业服务器的分片上下文.
     * 
     * @param shardingItems 分片项
     * @return 分片上下文
     */
    public ShardingContexts getJobShardingContext(final List<Integer> shardingItems) {
        LiteJobConfiguration liteJobConfig = configService.load(false);
        // 如果开启了监控作业运行时状态，从注册中心获取正在执行的分片项，然后从shardingItems移除
        removeRunningIfMonitorExecution(liteJobConfig.isMonitorExecution(), shardingItems);
        if (shardingItems.isEmpty()) {
            // 构建分片上下文
            return new ShardingContexts(
                    buildTaskId(liteJobConfig, shardingItems),
                    liteJobConfig.getJobName(),
                    liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(),
                    liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(),
                    Collections.<Integer, String>emptyMap()
            );
        }

        // 将分片参数的字符串配置解析为对应的Map数据接口，Map<分片项，参数值>
        Map<Integer, String> shardingItemParameterMap = new ShardingItemParameters(liteJobConfig.getTypeConfig().getCoreConfig().getShardingItemParameters()).getMap();
        return new ShardingContexts(
                buildTaskId(liteJobConfig, shardingItems),
                liteJobConfig.getJobName(),
                liteJobConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(),
                liteJobConfig.getTypeConfig().getCoreConfig().getJobParameter(),
                getAssignedShardingItemParameterMap(shardingItems, shardingItemParameterMap)
        );
    }

    /**
     * 获取任务的ID，格式：${ip}@-@${pid}，即ip地址 + 进程id
     *
     * @param liteJobConfig
     * @param shardingItems
     * @return
     */
    private String buildTaskId(final LiteJobConfiguration liteJobConfig, final List<Integer> shardingItems) {
        JobInstance jobInstance = JobRegistry.getInstance().getJobInstance(jobName);
        return Joiner.on("@-@").join(liteJobConfig.getJobName(), Joiner.on(",").join(shardingItems), "READY", 
                null == jobInstance.getJobInstanceId() ? "127.0.0.1@-@1" : jobInstance.getJobInstanceId()); 
    }

    /**
     * 如果开启了监控作业运行时状态，从注册中心获取正在执行的分片项，然后从shardingItems移除
     *
     * @param monitorExecution  是否开启监控作业运行时状态
     * @param shardingItems     分片项
     */
    private void removeRunningIfMonitorExecution(final boolean monitorExecution, final List<Integer> shardingItems) {
        if (!monitorExecution) {
            return;
        }

        List<Integer> runningShardingItems = new ArrayList<>(shardingItems.size());
        for (int each : shardingItems) {
            if (isRunning(each)) {
                runningShardingItems.add(each);
            }
        }
        shardingItems.removeAll(runningShardingItems);
    }

    /**
     * 判断该分片项对应的作业实例是否在运行，原理：判断zk上的 /${jobName}/sharding/${shardingItem}/running 节点是否存在，如果存在表示作业正在运行
     *
     * @param shardingItem
     * @return
     */
    private boolean isRunning(final int shardingItem) {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.getRunningNode(shardingItem));
    }

    /**
     * 获取分片项对应的分片参数
     *
     * @param shardingItems                 分片项集合
     * @param shardingItemParameterMap      所有的分片项及对应的参数配置
     * @return
     */
    private Map<Integer, String> getAssignedShardingItemParameterMap(final List<Integer> shardingItems, final Map<Integer, String> shardingItemParameterMap) {
        Map<Integer, String> result = new HashMap<>(shardingItemParameterMap.size(), 1);
        for (int each : shardingItems) {
            result.put(each, shardingItemParameterMap.get(each));
        }
        return result;
    }

}
