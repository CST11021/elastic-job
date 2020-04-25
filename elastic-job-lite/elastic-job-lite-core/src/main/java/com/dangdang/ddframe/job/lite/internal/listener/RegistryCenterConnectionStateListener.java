package com.dangdang.ddframe.job.lite.internal.listener;

import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceService;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.schedule.JobScheduleController;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * 注册中心连接状态监听器.
 *
 * @author zhangliang
 */
public final class RegistryCenterConnectionStateListener implements ConnectionStateListener {

    /** 作业名称 */
    private final String jobName;
    
    private final ServerService serverService;
    
    private final InstanceService instanceService;
    
    private final ShardingService shardingService;
    
    private final ExecutionService executionService;
    
    public RegistryCenterConnectionStateListener(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
    }

    /**
     * 监听zk客户端的连接状态，当连接丢失的时候，将作业暂停
     *
     *
     * @param client
     * @param newState
     */
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
        if (JobRegistry.getInstance().isShutdown(jobName)) {
            return;
        }

        JobScheduleController jobScheduleController = JobRegistry.getInstance().getJobScheduleController(jobName);

        // SUSPENDED:表示连接丢失但是连接尚未超时的时候
        // LOST:表示连接丢失了
        if (ConnectionState.SUSPENDED == newState || ConnectionState.LOST == newState) {
            // 暂停作业
            jobScheduleController.pauseJob();
        } else if (ConnectionState.RECONNECTED == newState) {

            // 1、如果servers/${ip}没有启动的话，设置为启动状态：节点数据为""表示启动，节点数据为DISABLED表示未启动
            // 获取本地IP地址
            String ip = JobRegistry.getInstance().getJobInstance(jobName).getIp();
            // 连接重连时，如果根据zk之前的配置，看看是否要启用或禁用作业服务
            boolean flag = serverService.isEnableServer(ip);
            serverService.persistOnline(flag);

            // 2、将作业实例保存到zk
            instanceService.persistOnline();

            executionService.clearRunningInfo(shardingService.getLocalShardingItems());
            jobScheduleController.resumeJob();
        }
    }
}
