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

import com.dangdang.ddframe.job.lite.internal.election.LeaderNode;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodePath;

/**
 * 分片节点路径：
 *
 * /${appName}/${jobName}/sharding
 *      /0
 *          /instance
 *          /misfire
 *          /running
 *      /1
 *          /instance
 *          /misfire
 *          /running
 *
 * 
 * @author zhangliang
 */
public final class ShardingNode {
    
    /** 执行状态根节点. */
    public static final String ROOT = "sharding";
    static final String INSTANCE_APPENDIX = "instance";
    static final String RUNNING_APPENDIX = "running";

    /** sharding/%s/instance */
    public static final String INSTANCE = ROOT + "/%s/" + INSTANCE_APPENDIX;
    /** sharding/%s/running */
    static final String RUNNING = ROOT + "/%s/" + RUNNING_APPENDIX;
    /** sharding/%s/misfire */
    static final String MISFIRE = ROOT + "/%s/misfire";
    /** sharding/%s/disabled */
    static final String DISABLED = ROOT + "/%s/disabled";


    // /${appName}/${jobName}/leader/sharding/

    /** leader/sharding */
    static final String LEADER_ROOT = LeaderNode.ROOT + "/" + ROOT;
    /** leader/sharding/necessary */
    static final String NECESSARY = LEADER_ROOT + "/necessary";
    /** leader/sharding/processing */
    static final String PROCESSING = LEADER_ROOT + "/processing";

    /** 用于获取zk节点的路径类 */
    private final JobNodePath jobNodePath;



    public ShardingNode(final String jobName) {
        jobNodePath = new JobNodePath(jobName);
    }

    /**
     * sharding/%s/instance
     *
     * @param item  分片索引，从0开始
     * @return
     */
    public static String getInstanceNode(final int item) {
        return String.format(INSTANCE, item);
    }



    /**
     * 获取作业运行状态节点路径，例如：
     * sharding/0/running
     * sharding/1/running
     *
     * @param item 分片索引
     * @return 作业运行状态节点路径
     */
    public static String getRunningNode(final int item) {
        return String.format(RUNNING, item);
    }

    /**
     * 返回例如：
     * sharding/0/misfire
     * sharding/1/misfire
     *
     * @param item
     * @return
     */
    static String getMisfireNode(final int item) {
        return String.format(MISFIRE, item);
    }

    /**
     * 返回例如：
     * sharding/0/disabled
     * sharding/1/disabled
     *
     * @param item
     * @return
     */
    static String getDisabledNode(final int item) {
        return String.format(DISABLED, item);
    }
    
    /**
     * 根据运行中的分片路径获取分片项，例如：/testJob/sharding/0/running，则返回：0
     *
     * @param path 运行中的分片路径
     * @return 分片项, 不是运行中的分片路径获则返回null
     */
    public Integer getItemByRunningItemPath(final String path) {
        if (!isRunningItemPath(path)) {
            return null;
        }

        return Integer.parseInt(path.substring(jobNodePath.getFullPath(ROOT).length() + 1, path.lastIndexOf(RUNNING_APPENDIX) - 1));
    }

    /**
     * 判断节点路径是否/${jobName}/sharding开头，running结尾
     *
     * @param path
     * @return
     */
    private boolean isRunningItemPath(final String path) {
        return path.startsWith(jobNodePath.getFullPath(ROOT)) && path.endsWith(RUNNING_APPENDIX);
    }
}
