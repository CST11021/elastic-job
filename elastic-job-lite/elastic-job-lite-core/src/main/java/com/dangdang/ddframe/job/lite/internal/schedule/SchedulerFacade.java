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

package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.election.LeaderService;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceService;
import com.dangdang.ddframe.job.lite.internal.listener.ListenerManager;
import com.dangdang.ddframe.job.lite.internal.monitor.MonitorService;
import com.dangdang.ddframe.job.lite.internal.reconcile.ReconcileService;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;

import java.util.List;

/**
 * 为调度器提供内部服务的门面类.
 * 
 * @author zhangliang
 */
public final class SchedulerFacade {

    /** 作业名称 */
    private final String jobName;
    
    private final ConfigurationService configService;
    
    private final LeaderService leaderService;
    
    private final ServerService serverService;
    
    private final InstanceService instanceService;
    
    private final ShardingService shardingService;
    
    private final ExecutionService executionService;
    
    private final MonitorService monitorService;
    
    private final ReconcileService reconcileService;

    /** 监听管理器类 */
    private ListenerManager listenerManager;
    
    public SchedulerFacade(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        monitorService = new MonitorService(regCenter, jobName);
        reconcileService = new ReconcileService(regCenter, jobName);
    }
    
    public SchedulerFacade(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners) {
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        monitorService = new MonitorService(regCenter, jobName);
        reconcileService = new ReconcileService(regCenter, jobName);
        listenerManager = new ListenerManager(regCenter, jobName, elasticJobListeners);
    }
    
    /**
     * 获取作业触发监听器.
     *
     * @return 作业触发监听器
     */
    public JobTriggerListener newJobTriggerListener() {
        return new JobTriggerListener(executionService, shardingService);
    }
    
    /**
     * 将作业配置保存到zk上的/config节点
     *
     * @param liteJobConfig 作业配置
     * @return 更新后的作业配置
     */
    public LiteJobConfiguration updateJobConfiguration(final LiteJobConfiguration liteJobConfig) {
        // 将作业配置保存到zk上的/config节点
        configService.persist(liteJobConfig);
        // 从zk读取作业配置
        return configService.load(false);
    }
    
    /**
     * 注册作业启动信息.
     * 
     * @param enabled 作业是否启用
     */
    public void registerStartUpInfo(final boolean enabled) {
        // 启动所有的监听器
        listenerManager.startAllListeners();

        // 多个作业实例，竞选主节点
        leaderService.electLeader();
        // enabled = false 时，设置servers/${ip}节点数据为DISABLED
        serverService.persistOnline(enabled);

        // 将作业实例保存到zk
        instanceService.persistOnline();

        // 设置分片标识
        shardingService.setReshardingFlag();
        monitorService.listen();
        if (!reconcileService.isRunning()) {
            reconcileService.startAsync();
        }
    }
    
    /**
     * 终止作业调度.
     */
    public void shutdownInstance() {
        // 主节点shutdown时，需要异常zk上主节点标记
        if (leaderService.isLeader()) {
            leaderService.removeLeader();
        }

        monitorService.close();
        if (reconcileService.isRunning()) {
            reconcileService.stopAsync();
        }
        JobRegistry.getInstance().shutdown(jobName);
    }
}
