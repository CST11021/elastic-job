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

package com.dangdang.ddframe.job.reg.zookeeper;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * 基于Zookeeper的注册中心配置.
 * 
 * @author zhangliang
 * @author caohao
 */
@Getter
@Setter
@RequiredArgsConstructor
public final class ZookeeperConfiguration {
    
    /** 连接Zookeeper服务器的列表, 包括IP地址和端口号, 多个地址用逗号分隔, 如: host1:2181,host2:2181 */
    private final String serverLists;
    
    /** 命名空间, 对应zk的Chroot, 一般配置为对应的应用名 */
    private final String namespace;
    
    /** 等待重试的间隔时间的初始值. 单位毫秒. */
    private int baseSleepTimeMilliseconds = 1000;
    
    /**
     * 等待重试的间隔时间的最大值. 单位毫秒, 例如：
     * 初始重试间隔为1000ms，那么一次失败后，等待1秒后继续第二次重试，
     * 如果第二次还是失败，需要等待1000ms * 2 = 2000ms,
     * 如果第三次还是失败，则需要等待 1000ms * 4 = 4000ms，
     * 这里超过3000，则zk客户端不会再尝试重试
     */
    private int maxSleepTimeMilliseconds = 3000;
    
    /** 最大重试次数, 客户端连接zk的重试次数, 重试操作3次不再尝试连接 */
    private int maxRetries = 3;
    
    /** 会话超时时间. 单位毫秒, 默认：60000. */
    private int sessionTimeoutMilliseconds;
    
    /** 连接超时时间, 单位毫秒, 默认：15000. */
    private int connectionTimeoutMilliseconds;
    
    /** 连接Zookeeper的权限令牌. 缺省为不需要权限验证. */
    private String digest;
}
