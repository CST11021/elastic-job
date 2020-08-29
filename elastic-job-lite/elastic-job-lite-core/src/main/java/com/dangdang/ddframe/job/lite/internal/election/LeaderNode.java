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

import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * 用于定义${jobName}/leader/下的节点路径
 * 
 * @author zhangliang
 */
public final class LeaderNode {
    
    /** 主节点根路径. */
    public static final String ROOT = "leader";
    /** leader/election */
    static final String ELECTION_ROOT = ROOT + "/election";
    /** leader/election/instance */
    static final String INSTANCE = ELECTION_ROOT + "/instance";
    /** leader/election/latch */
    static final String LATCH = ELECTION_ROOT + "/latch";
    
    private final JobNodePath jobNodePath;
    
    LeaderNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * 判断入参path是否等于：${jobName}/leader/election/instance
     *
     * @param path
     * @return
     */
    boolean isLeaderInstancePath(final String path) {
        return jobNodePath.getFullPath(INSTANCE).equals(path);
    }
}
