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

import com.dangdang.ddframe.job.lite.internal.config.ConfigurationNode;
import com.dangdang.ddframe.job.lite.internal.config.LiteJobConfigurationGsonFactory;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 分片监听管理器.
 * 
 * @author zhangliang
 */
public final class ShardingListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final ConfigurationNode configNode;
    
    private final InstanceNode instanceNode;
    
    private final ServerNode serverNode;
    
    private final ShardingService shardingService;
    
    public ShardingListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
        serverNode = new ServerNode(jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new ShardingTotalCountChangedJobListener());
        addDataListener(new ListenServersChangedJobListener());
    }

    /**
     * 监听/${jobName}/config的节点配置，当shardingTotalCount变更时，
     */
    class ShardingTotalCountChangedJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (configNode.isConfigPath(path) && 0 != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {
                int newShardingTotalCount = LiteJobConfigurationGsonFactory.fromJson(data).getTypeConfig().getCoreConfig().getShardingTotalCount();
                if (newShardingTotalCount != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {
                    shardingService.setReshardingFlag();
                    JobRegistry.getInstance().setCurrentShardingTotalCount(jobName, newShardingTotalCount);
                }
            }
        }
    }

    /**
     * 当该作业的作业实例列表发生变化时，也会触发重新分片
     */
    class ListenServersChangedJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isInstanceChange(eventType, path) || isServerChange(path))) {
                shardingService.setReshardingFlag();
            }
        }

        /**
         * 判断给定路径是否是/instances节点下面的，并且该path对应的节点发生了变更
         *
         * @param eventType
         * @param path
         * @return
         */
        private boolean isInstanceChange(final Type eventType, final String path) {
            return instanceNode.isInstancePath(path) && Type.NODE_UPDATED != eventType;
        }

        /**
         * 判断该path是否符合：${jobName}/servers/${ip} 规则
         *
         * @param path
         * @return
         */
        private boolean isServerChange(final String path) {
            return serverNode.isServerPath(path);
        }
    }
}
