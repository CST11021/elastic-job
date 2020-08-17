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

package com.dangdang.ddframe.job.lite.api.strategy;

import java.util.List;
import java.util.Map;

/**
 * 作业分片策略：
 * AverageAllocationJobShardingStrategy：基于平均分配算法的分片策略
 * OdevitySortByNameJobShardingStrategy：根据作业名的哈希值奇偶数决定IP升降序算法的分片策略
 * RotateServerByNameJobShardingStrategy：根据作业名的哈希值对服务器列表进行轮转的分片策略
 * 
 * @author zhangliang
 */
public interface JobShardingStrategy {
    
    /**
     * 作业分片.
     * 
     * @param jobInstances          所有参与分片的作业实例
     * @param jobName               作业名称
     * @param shardingTotalCount    分片总数
     * @return 分片结果，Map<作业实现，消费的分片项集合>
     */
    Map<JobInstance, List<Integer>> sharding(List<JobInstance> jobInstances, String jobName, int shardingTotalCount);
}
