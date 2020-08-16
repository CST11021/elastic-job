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

package com.dangdang.ddframe.job.lite.internal.storage;

import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.exception.RegExceptionHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionStateListener;

import java.util.List;

/**
 * 用于操作zk上的节点，例如：增删改查等
 *
 * @author zhangliang
 */
public final class JobNodeStorage {

    /** 任务名称 */
    private final String jobName;
    /** 注册中心 */
    private final CoordinatorRegistryCenter regCenter;
    /** 用于返回节点全路径的工具类，每个作业实例对应一个JobNodePath实例，作业节点的全路径是在预先约定的节点路径前加上作业名称 */
    private final JobNodePath jobNodePath;
    
    public JobNodeStorage(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.regCenter = regCenter;
        this.jobName = jobName;
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * 判断注册中心是否创建了某节点
     * 
     * @param node 节点名
     * @return 节点是否存在
     */
    public boolean isJobNodeExisted(final String node) {
        // 判断 /${jobName}/${node} 是否存在
        return regCenter.isExisted(jobNodePath.getFullPath(node));
    }
    
    /**
     * 获取某个节点的数据.
     * 
     * @param node 节点名
     * @return 返回节点数据
     */
    public String getJobNodeData(final String node) {
        // 获取 /${jobName}/${node} 节点数据
        return regCenter.get(jobNodePath.getFullPath(node));
    }
    
    /**
     * 直接从注册中心而非本地缓存获取节点数据.
     * 
     * @param node 节点名称
     * @return 节点数据值
     */
    public String getJobNodeDataDirectly(final String node) {
        return regCenter.getDirectly(jobNodePath.getFullPath(node));
    }
    
    /**
     * 获取节点的子节点名称列表.
     * 
     * @param node 节点名称
     * @return 节点的子节点名称列表
     */
    public List<String> getJobNodeChildrenKeys(final String node) {
        return regCenter.getChildrenKeys(jobNodePath.getFullPath(node));
    }
    
    /**
     * 如果节点不存在，则创建一个节点，注意：如果作业根节点不存在表示作业已经停止, 不再继续创建节点
     * 
     * @param node 节点名称
     */
    public void createJobNodeIfNeeded(final String node) {
        // 如果 /${jobName} 节点存在，并且 /${jobName}/${node} 节点不存在，则创建一个 /${jobName}/${node} 节点
        if (isJobRootNodeExisted() && !isJobNodeExisted(node)) {
            regCenter.persist(jobNodePath.getFullPath(node), "");
        }
    }

    /**
     * 判断作业根节点，即 /${jobName} 节点是否存在
     *
     * @return
     */
    private boolean isJobRootNodeExisted() {
        return regCenter.isExisted("/" + jobName);
    }
    
    /**
     * 删除节点.
     * 
     * @param node 节点名称
     */
    public void removeJobNodeIfExisted(final String node) {
        if (isJobNodeExisted(node)) {
            regCenter.remove(jobNodePath.getFullPath(node));
        }
    }
        
    /**
     * 填充节点数据.
     *
     * @param node  节点名称
     * @param value 节点数据值
     */
    public void fillJobNode(final String node, final Object value) {
        regCenter.persist(jobNodePath.getFullPath(node), value.toString());
    }
    
    /**
     * 填充临时节点数据.
     * 
     * @param node      节点名称
     * @param value     节点数据值
     */
    public void fillEphemeralJobNode(final String node, final Object value) {
        regCenter.persistEphemeral(jobNodePath.getFullPath(node), value.toString());
    }
    
    /**
     * 更新节点数据.
     * 
     * @param node  节点名称
     * @param value 节点数据值
     */
    public void updateJobNode(final String node, final Object value) {
        regCenter.update(jobNodePath.getFullPath(node), value.toString());
    }
    
    /**
     * 替换作业节点数据，通过该接口，将作业配置，保存到/config节点（注意是持久节点）
     * 
     * @param node  节点名称
     * @param value 待替换的数据
     */
    public void replaceJobNode(final String node, final Object value) {
        regCenter.persist(jobNodePath.getFullPath(node), value.toString());
    }

    /**
     * 在事务中执行操作.
     * 
     * @param callback 执行操作的回调
     */
    public void executeInTransaction(final TransactionExecutionCallback callback) {
        try {
            CuratorTransactionFinal curatorTransactionFinal = getClient().inTransaction().check().forPath("/").and();
            callback.execute(curatorTransactionFinal);
            curatorTransactionFinal.commit();
        } catch (final Exception ex) {
            RegExceptionHandler.handleException(ex);
        }
    }
    
    /**
     * 在主节点执行操作，先获取分布式锁，如果获取到了，则指定回调操作
     * 
     * @param latchNode     分布式锁使用的作业节点名称：leader/election/latch
     * @param callback      执行操作的回调
     */
    public void executeInLeader(final String latchNode, final LeaderExecutionCallback callback) {
        // 创建一个分布式锁，然后开始等待，直到获取成功
        try (LeaderLatch latch = new LeaderLatch(getClient(), jobNodePath.getFullPath(latchNode))) {
            latch.start();
            latch.await();
            // 获取分布式锁成功后，开始指定回调操作
            callback.execute();
        } catch (final Exception ex) {
            handleException(ex);
        }
    }

    /**
     * 处理回调异常
     *
     * @param ex
     */
    private void handleException(final Exception ex) {
        if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else {
            throw new JobSystemException(ex);
        }
    }
    
    /**
     * 注册zk连接状态的监听器.
     * 
     * @param listener 连接状态监听器
     */
    public void addConnectionStateListener(final ConnectionStateListener listener) {
        getClient().getConnectionStateListenable().addListener(listener);
    }

    /**
     * 获取zk客户端，这里使用的是Curator客户端
     *
     * @return
     */
    private CuratorFramework getClient() {
        return (CuratorFramework) regCenter.getRawClient();
    }
    
    /**
     * 注册数据监听器.
     * 
     * @param listener 数据监听器
     */
    public void addDataListener(final TreeCacheListener listener) {
        TreeCache cache = (TreeCache) regCenter.getRawCache("/" + jobName);
        cache.getListenable().addListener(listener);
    }
    
    /**
     * 获取注册中心机器的当前时间
     * 
     * @return 注册中心当前时间
     */
    public long getRegistryCenterTime() {
        return regCenter.getRegistryCenterTime(jobNodePath.getFullPath("systemTime/current"));
    }
}
