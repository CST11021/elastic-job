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

package com.dangdang.ddframe.job.lite.internal.instance;

import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.SchedulerFacade;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 运行实例关闭监听管理器.
 * 
 * @author zhangliang
 */
public final class ShutdownListenerManager extends AbstractListenerManager {

    /** 监听的作业 */
    private final String jobName;
    
    private final InstanceNode instanceNode;
    
    private final InstanceService instanceService;
    
    private final SchedulerFacade schedulerFacade;
    
    public ShutdownListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        instanceNode = new InstanceNode(jobName);
        instanceService = new InstanceService(regCenter, jobName);
        schedulerFacade = new SchedulerFacade(regCenter, jobName);
    }
    
    @Override
    public void start() {
        addDataListener(new InstanceShutdownStatusJobListener());
    }

    /**
     * 监听/instances/172.16.120.135@-@97708节点，如果该节点被移除，则将作业实例shutdown
     */
    class InstanceShutdownStatusJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && !JobRegistry.getInstance().getJobScheduleController(jobName).isPaused()
                    && isRemoveInstance(path, eventType) && !isReconnectedRegistryCenter()) {
                schedulerFacade.shutdownInstance();
            }
        }

        /**
         * 判断/instances/172.16.120.135@-@97708该节点是否被移除
         *
         * @param path
         * @param eventType
         * @return
         */
        private boolean isRemoveInstance(final String path, final Type eventType) {
            return instanceNode.isLocalInstancePath(path) && Type.NODE_REMOVED == eventType;
        }

        /**
         * 判断注册中心该作业节点是否存在
         *
         * @return
         */
        private boolean isReconnectedRegistryCenter() {
            return instanceService.isLocalJobInstanceExisted();
        }
    }
}
