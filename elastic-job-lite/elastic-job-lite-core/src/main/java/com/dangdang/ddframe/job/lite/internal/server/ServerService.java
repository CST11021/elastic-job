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

package com.dangdang.ddframe.job.lite.internal.server;

import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;

import java.util.List;

/**
 * 作业服务器服务.
 * 
 * @author zhangliang
 * @author caohao
 */
public final class ServerService {
    /** 作业名称 */
    private final String jobName;
    /** 作业节点数据访问类 */
    private final JobNodeStorage jobNodeStorage;
    /** 用于获取servers下的节点路径 */
    private final ServerNode serverNode;
    
    public ServerService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverNode = new ServerNode(jobName);
    }
    
    /**
     * 持久化作业服务器上线信息，
     * 当enabled = true 时：设置servers/${ip}路径的value为空串
     * 当enabled = false 时：设置servers/${ip}路径的value为DISABLED
     *
     * @param enabled 作业是否启用
     */
    public void persistOnline(final boolean enabled) {
        if (!JobRegistry.getInstance().isShutdown(jobName)) {
            // 获取节点路径：servers/${ip}
            String path = serverNode.getServerNode(JobRegistry.getInstance().getJobInstance(jobName).getIp());
            // 当zk上的servers/${ip}节点的ip和本地实例的IP不一致时，设置节点数据为DISABLED
            jobNodeStorage.fillJobNode(path, enabled ? "" : ServerStatus.DISABLED.name());
        }
    }
    
    /**
     * 获取是否还有可用的作业服务器.
     * 
     * @return 是否还有可用的作业服务器
     */
    public boolean hasAvailableServers() {
        List<String> servers = jobNodeStorage.getJobNodeChildrenKeys(ServerNode.ROOT);
        for (String each : servers) {
            if (isAvailableServer(each)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 判断作业服务器是否可用.
     *
     * 1、判断zk上的servers/${ip}节点的ip和入参的ip是否一致
     * 2、判断zk上instances/下的是否存在该ip
     * 
     * @param ip 作业服务器IP地址
     * @return 作业服务器是否可用
     */
    public boolean isAvailableServer(final String ip) {
        return isEnableServer(ip) && hasOnlineInstances(ip);
    }

    /**
     * 判断zk上instances/下的是否存在该ip
     *
     * @param ip
     * @return
     */
    private boolean hasOnlineInstances(final String ip) {
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT)) {
            if (each.startsWith(ip)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 判断zk上的servers/${ip}节点的ip和入参的ip是否一致
     *
     * @param ip 作业服务器IP地址
     * @return 服务器是否启用
     */
    public boolean isEnableServer(final String ip) {
        return !ServerStatus.DISABLED.name().equals(jobNodeStorage.getJobNodeData(serverNode.getServerNode(ip)));
    }
}
