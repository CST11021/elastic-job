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

import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * 运行实例节点路径.
 * 
 * @author zhangliang
 */
public final class InstanceNode {
    
    /** 运行实例信息根节点. */
    public static final String ROOT = "instances";
    /** instances/%s */
    private static final String INSTANCES = ROOT + "/%s";
    /** 作业名称 */
    private final String jobName;
    /** 用于获取zk上作业节点的路径类 */
    private final JobNodePath jobNodePath;
    
    public InstanceNode(final String jobName) {
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 获取作业运行实例全路径.
     *
     * @return 作业运行实例全路径
     */
    public String getInstanceFullPath() {
        return jobNodePath.getFullPath(InstanceNode.ROOT);
    }
    
    /**
     * 判断给定路径是否是/instances节点下面的
     *
     * @param path 待判断的路径
     * @return 是否为作业运行实例路径
     */
    public boolean isInstancePath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(InstanceNode.ROOT));
    }

    /**
     * 判断 path 是否对应该InstanceNode实例，zk保存的路径如： ${namaSpace}/${jobName}/instances/172.16.120.135@-@97708
     *
     * @param path
     * @return
     */
    boolean isLocalInstancePath(final String path) {
        return path.equals(jobNodePath.getFullPath(String.format(INSTANCES, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())));
    }

    /**
     * 返回例如：instances/172.16.120.135@-@97544
     *
     * @return
     */
    String getLocalInstanceNode() {
        return String.format(INSTANCES, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
}
