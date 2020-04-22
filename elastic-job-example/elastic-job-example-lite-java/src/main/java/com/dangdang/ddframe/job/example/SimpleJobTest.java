package com.dangdang.ddframe.job.example;

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.example.job.simple.JavaSimpleJob;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * @Author: wanghz
 * @Date: 2020/4/22 9:57 AM
 */
public class SimpleJobTest {

    public static void main(String[] args) {
        // 启动一个zk服务
        EmbedZookeeperServer.start(4181);

        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration("localhost:4181", "simpleJobTest");
        CoordinatorRegistryCenter regCenter = new ZookeeperRegistryCenter(zkConfig);
        regCenter.init();

        // 1、配置一个作业任务
        JobCoreConfiguration coreConfig = JobCoreConfiguration
                .newBuilder("javaSimpleJob", "0/5 * * * * ?", 3)
                .shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou")
                .build();
        // 2、创建一个简单类型的作业任务
        JobTypeConfiguration jobTypeConfig = new SimpleJobConfiguration(coreConfig, JavaSimpleJob.class.getCanonicalName());
        // 3、作业额外的一些配置，比如：分片策略、是否禁用等
        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(jobTypeConfig).build();

        // 二、启动作业调度器
        new JobScheduler(regCenter, liteJobConfiguration).init();
    }

}
