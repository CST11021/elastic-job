package com.dangdang.ddframe.job.example;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.example.fixture.entity.Foo;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: wanghz
 * @Date: 2020/4/22 9:57 AM
 */
public class SimpleJobTest implements SimpleJob {

    public static void main(String[] args) {
        // 启动一个zk服务
        EmbedZookeeperServer.start(4181);

        // 启动注册中心客户端
        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration("localhost:4181", "simpleJobTest");
        CoordinatorRegistryCenter regCenter = new ZookeeperRegistryCenter(zkConfig);
        regCenter.init();

        // 1、配置一个作业任务
        JobCoreConfiguration coreConfig = JobCoreConfiguration
                .newBuilder("javaSimpleJob", "0/5 * * * * ?", 3)
                .shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou")
                .build();
        // 2、创建一个简单类型的作业任务
        JobTypeConfiguration jobTypeConfig = new SimpleJobConfiguration(coreConfig, SimpleJobTest.class.getCanonicalName());
        // 3、作业额外的一些配置，比如：分片策略、是否禁用等
        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(jobTypeConfig).build();

        // 二、启动作业调度器
        new JobScheduler(regCenter, liteJobConfiguration).init();
    }

    @Override
    public void execute(ShardingContext shardingContext) {
        String startTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        System.out.println(String.format("分片索引: %s | 执行时间: %s | 线程: %s",
                shardingContext.getShardingItem(), startTime, Thread.currentThread().getId()));
    }
}
