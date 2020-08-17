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

package com.dangdang.ddframe.job.lite.internal.config;

import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * 作业节点的配置类，用于获取zk上的作业配置节点路径
 * 
 * @author zhangliang
 */
public final class ConfigurationNode {

    /** 作业节点的跟路径 */
    static final String ROOT = "config";
    /** 用于获取对应作业在zk上的配置节点路径 */
    private final JobNodePath jobNodePath;
    
    public ConfigurationNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 判断该路径是否对应 /${jobName}/config 路径
     * 
     * @param path 节点路径
     * @return 是否为作业配置根路径
     */
    public boolean isConfigPath(final String path) {
        return jobNodePath.getConfigNodePath().equals(path);
    }
}
