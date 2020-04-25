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

package com.dangdang.ddframe.job.lite.internal.election;

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.lite.internal.storage.LeaderExecutionCallback;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.util.concurrent.BlockUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 主节点服务.
 * 
 * @author zhangliang
 */
@Slf4j
public final class LeaderService {

    /** 作业名称 */
    private final String jobName;
    /** 作业服务器服务 */
    private final ServerService serverService;
    /** 作业节点数据访问类 */
    private final JobNodeStorage jobNodeStorage;


    public LeaderService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    /**
     * 选举主节点.
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        // 获取 leader/election/latch 这个分布式锁，如果获取成功，则执行回调
        jobNodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.debug("Leader election completed.");
    }
    
    /**
     * 判断当前节点是否是主节点.
     * 
     * <p>
     * 如果主节点正在选举中而导致取不到主节点, 则阻塞至主节点选举完成再返回.
     * </p>
     * 
     * @return 当前节点是否是主节点
     */
    public boolean isLeaderUntilBlock() {
        while (!hasLeader() && serverService.hasAvailableServers()) {
            log.info("Leader is electing, waiting for {} ms", 100);
            BlockUtils.waitingShortTime();
            if (!JobRegistry.getInstance().isShutdown(jobName) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
                electLeader();
            }
        }
        return isLeader();
    }
    
    /**
     * 判断当前该作业实例是否为主节点，判断是否是主节点：leader/election/instance节点的数据 == jobInstanceId
     *
     * @return 当前节点是否是主节点
     */
    public boolean isLeader() {
        return !JobRegistry.getInstance().isShutdown(jobName) &&
                JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }
    
    /**
     * 判断是否已经有主节点，判断leader/election/instance节点是否存在
     * 
     * @return 是否已经有主节点
     */
    public boolean hasLeader() {
        return jobNodeStorage.isJobNodeExisted(LeaderNode.INSTANCE);
    }
    
    /**
     * 删除主节点供重新选举.
     */
    public void removeLeader() {
        // 删除 leader/election/instance 节点
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }

    /**
     * 通过分布锁，选举主节点：多台机器去竞争分布式锁"leader/election/latch"，谁获取成功，则将该机器的作业实例主键，设置到leader/election/instance下
     */
    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {

        @Override
        public void execute() {
            if (!hasLeader()) {
                // 根据作业名，获取作业实例主键，例如：172.16.120.135@-@97544
                String jobInstanceId = JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId();
                // 设置leader/election/instance节点的值为该主键
                jobNodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, jobInstanceId);
            }
        }
    }

}
