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

package com.dangdang.ddframe.job.lite.internal.listener;

import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

/**
 * 作业注册中心的监听器管理者的抽象类，Elastic-job中的所有监听器都继承该抽象类，注意，该监听器都基于作业维度的监听器，例如：
 * ElectionListenerManager：主节点选举监听管理器
 * FailoverListenerManager：失效转移监听管理器
 * GuaranteeListenerManager：保证分布式任务全部开始和结束状态监听管理器
 * MonitorExecutionListenerManager：幂等性监听管理器
 * RescheduleListenerManager：重调度监听管理器
 * ShardingListenerManager：分片监听管理器
 * ShutdownListenerManager：运行实例关闭监听管理器
 * TriggerListenerManager：作业触发监听管理器
 *
 * 
 * @author zhangliang
 */
public abstract class AbstractListenerManager {

    /** 用于作业配置数据存储 */
    private final JobNodeStorage jobNodeStorage;
    
    protected AbstractListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
    }

    /**
     * 开启监听器.
     */
    public abstract void start();

    /**
     * 添加监听器
     *
     * @param listener
     */
    protected void addDataListener(final TreeCacheListener listener) {
        jobNodeStorage.addDataListener(listener);
    }
}
