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

package com.dangdang.ddframe.job.event;

import com.dangdang.ddframe.job.util.concurrent.ExecutorServiceObject;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;

/**
 * 运行痕迹事件总线：用于注册事件监听和发布事件
 * 
 * @author zhangliang
 * @author caohao
 */
@Slf4j
public final class JobEventBus {

    /** 作业事件配置 */
    private final JobEventConfiguration jobEventConfig;

    /** 线程池服务 */
    private final ExecutorServiceObject executorServiceObject;

    /** 事件总线 */
    private final EventBus eventBus;

    /** 表示该事件实例是否已经注册 */
    private boolean isRegistered;
    
    public JobEventBus() {
        jobEventConfig = null;
        executorServiceObject = null;
        eventBus = null;
    }
    
    public JobEventBus(final JobEventConfiguration jobEventConfig) {
        this.jobEventConfig = jobEventConfig;
        executorServiceObject = new ExecutorServiceObject("job-event", Runtime.getRuntime().availableProcessors() * 2);
        eventBus = new AsyncEventBus(executorServiceObject.createExecutorService());
        register();
    }

    /**
     * 注册该事件的监听器
     */
    private void register() {
        try {
            eventBus.register(jobEventConfig.createJobEventListener());
            isRegistered = true;
        } catch (final JobEventListenerConfigurationException ex) {
            log.error("Elastic job: create JobEventListener failure, error is: ", ex);
        }
    }
    
    /**
     * 发布一个作业事件.
     *
     * @param event 作业事件
     */
    public void post(final JobEvent event) {
        if (isRegistered && !executorServiceObject.isShutdown()) {
            eventBus.post(event);
        }
    }
}
