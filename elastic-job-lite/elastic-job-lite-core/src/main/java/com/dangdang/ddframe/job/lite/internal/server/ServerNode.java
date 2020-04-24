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

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;
import com.dangdang.ddframe.job.util.env.IpUtils;

import java.util.regex.Pattern;

/**
 * 服务器节点路径.
 * 
 * @author zhangliang
 */
public final class ServerNode {

    /** 作业名称 */
    private final String jobName;
    /** 服务器信息根节点. */
    public static final String ROOT = "servers";
    /** servers/%s */
    private static final String SERVERS = ROOT + "/%s";
    /** 用于获取zk上的节点路径类 */
    private final JobNodePath jobNodePath;
    
    public ServerNode(final String jobName) {
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 判断给定路径是否为作业服务器路径：判断该path是否符合：${jobName}/servers/${ip} 规则
     *
     * @param path 待判断的路径
     * @return 是否为作业服务器路径
     */
    public boolean isServerPath(final String path) {
        return Pattern.compile(jobNodePath.getFullPath(ServerNode.ROOT) + "/" + IpUtils.IP_REGEX).matcher(path).matches();
    }
    
    /**
     * 判断给定路径是否为本地作业服务器路径：判断path是否为该job实例的ip
     *
     * @param path 待判断的路径
     * @return 是否为本地作业服务器路径
     */
    public boolean isLocalServerPath(final String path) {
        // 该作业实际在zk上的 ${jobName}/servers/${ip} 是否和path一致
        return path.equals(jobNodePath.getFullPath(String.format(SERVERS, JobRegistry.getInstance().getJobInstance(jobName).getIp())));
    }

    /**
     * 返回 servers/${ip}
     *
     * @param ip
     * @return
     */
    String getServerNode(final String ip) {
        return String.format(SERVERS, ip);
    }
}
