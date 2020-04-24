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

import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.server.ServerStatus;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 主节点选举监听管理器.
 * 
 * @author zhangliang
 */
public final class ElectionListenerManager extends AbstractListenerManager {

    /** 作业名称 */
    private final String jobName;
    
    private final LeaderNode leaderNode;
    
    private final ServerNode serverNode;
    
    private final LeaderService leaderService;
    
    private final ServerService serverService;
    
    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        leaderNode = new LeaderNode(jobName);
        serverNode = new ServerNode(jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new LeaderElectionJobListener());
        addDataListener(new LeaderAbdicationJobListener());
    }

    /**
     * Leader节点选举监听
     */
    class LeaderElectionJobListener extends AbstractJobListener {

        /**
         * 节点数据变更时触发，leader/election/instance节点被移除 && servers/${ip}节点的数据不是DISABLED &&
         *
         * @param path          节点路径
         * @param eventType     事件类型
         * @param data
         */
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {

            if (!JobRegistry.getInstance().isShutdown(jobName) && (isActiveElection(path, data) || isPassiveElection(path, eventType))) {
                leaderService.electLeader();
            }

        }

        /**
         * 是否触发选举：如果leader/election/instance节点不存在 && servers/${ip}节点的数据不是DISABLED
         *
         * @param path
         * @param data
         * @return
         */
        private boolean isActiveElection(final String path, final String data) {
            // 如果leader/election/instance节点不存在 && servers/${ip}节点的数据不是DISABLED
            return !leaderService.hasLeader() && isLocalServerEnabled(path, data);
        }

        /**
         * 被动触发选举：当leader节点故障，并且本地作业实例ip是合法的
         *
         * @param path
         * @param eventType
         * @return
         */
        private boolean isPassiveElection(final String path, final Type eventType) {
            // 当leader节点故障，并且本地作业实例ip是合法的
            return isLeaderCrashed(path, eventType) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp());
        }

        /**
         * 当节点${jobName}/leader/election/instance被移除，说明Leader节点故障了
         *
         * @param path
         * @param eventType
         * @return
         */
        private boolean isLeaderCrashed(final String path, final Type eventType) {
            return leaderNode.isLeaderInstancePath(path) && Type.NODE_REMOVED == eventType;
        }

        /**
         * 是否开启作业实例，servers/${ip} 节点的数据不是DISABLED
         *
         * @param path
         * @param data
         * @return
         */
        private boolean isLocalServerEnabled(final String path, final String data) {
            // servers/${ip} 节点的数据不是DISABLED
            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
        }
    }

    /**
     * 移除Leader节点监听：leader节点的作业停止了，则移除Leader节点
     */
    class LeaderAbdicationJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            // leader节点的作业停止了，则移除Leader节点
            if (leaderService.isLeader() && isLocalServerDisabled(path, data)) {
                leaderService.removeLeader();
            }
        }

        /**
         * 判断本地作业是否被禁用了
         *
         * @param path
         * @param data
         * @return
         */
        private boolean isLocalServerDisabled(final String path, final String data) {
            // 该path对应该作业实例的配置 && 作业被DISABLED
            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
        }
    }
}
