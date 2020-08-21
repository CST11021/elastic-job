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

package com.dangdang.ddframe.job.example;

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.JobEventConfiguration;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbConfiguration;
import com.dangdang.ddframe.job.example.job.dataflow.JavaDataflowJob;
import com.dangdang.ddframe.job.example.job.simple.JavaSimpleJob;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import org.apache.commons.dbcp.BasicDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

public final class JavaMain {

    // zk配置

    /** zk端口 */
    private static final int EMBED_ZOOKEEPER_PORT = 4181;
    /** zk地址 */
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:" + EMBED_ZOOKEEPER_PORT;
    /** 隔离命名空间，对应zk的Chroot */
    private static final String JOB_NAMESPACE = "elastic-job-example-lite-java";

    // h2数据源设置

    private static final String EVENT_RDB_STORAGE_DRIVER = "org.h2.Driver";
    private static final String EVENT_RDB_STORAGE_URL = "jdbc:h2:mem:job_event_storage";
    private static final String EVENT_RDB_STORAGE_USERNAME = "sa";
    private static final String EVENT_RDB_STORAGE_PASSWORD = "";

    // switch to MySQL by yourself
    // private static final String EVENT_RDB_STORAGE_DRIVER = "com.mysql.jdbc.Driver";
    // private static final String EVENT_RDB_STORAGE_URL = "jdbc:mysql://localhost:3306/elastic_job_log";

    
    public static void main(final String[] args) throws IOException {
        // 启动一个zk服务
        EmbedZookeeperServer.start(EMBED_ZOOKEEPER_PORT);

        // 启动注册中心：连接zk服务
        CoordinatorRegistryCenter regCenter = setUpRegistryCenter();

        // 作业事件配置：这里使用Rdb事件监听机制，通过配置数据源，将作业事件保存到数据库
        JobEventConfiguration jobEventConfig = new JobEventRdbConfiguration(setUpEventTraceDataSource());

        // 启动任务
        setUpSimpleJob(regCenter, jobEventConfig);

        // setUpDataflowJob(regCenter, jobEventConfig);
        // setUpScriptJob(regCenter, jobEventConfig);
    }

    /**
     * 启动注册中心
     *
     * @return
     */
    private static CoordinatorRegistryCenter setUpRegistryCenter() {
        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(ZOOKEEPER_CONNECTION_STRING, JOB_NAMESPACE);
        CoordinatorRegistryCenter result = new ZookeeperRegistryCenter(zkConfig);
        result.init();
        return result;
    }

    /**
     * 设置数据源
     *
     * @return
     */
    private static DataSource setUpEventTraceDataSource() {
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(EVENT_RDB_STORAGE_DRIVER);
        result.setUrl(EVENT_RDB_STORAGE_URL);
        result.setUsername(EVENT_RDB_STORAGE_USERNAME);
        result.setPassword(EVENT_RDB_STORAGE_PASSWORD);
        return result;
    }

    /**
     * 启动一个SimpleJob
     *
     * @param regCenter         作业注册中心
     * @param jobEventConfig    作业事件，用于保存作业执行的日志
     */
    private static void setUpSimpleJob(final CoordinatorRegistryCenter regCenter, final JobEventConfiguration jobEventConfig) {

        // 一、创建一个作业任务

        // 1、配置一个作业任务
        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder("javaSimpleJob", "0/1 * * * * ?", 3)
                .shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou")
                .build();
        // 2、创建一个简单类型的作业任务
        JobTypeConfiguration jobTypeConfig = new SimpleJobConfiguration(coreConfig, JavaSimpleJob.class.getCanonicalName());
        // 3、作业额外的一些配置，比如：分片策略、是否禁用等
        LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(jobTypeConfig).overwrite(true).build();

        // 二、启动作业调度器
        JobScheduler jobScheduler = new JobScheduler(regCenter, liteJobConfiguration, jobEventConfig);
        jobScheduler.init();
    }

    /**
     * 启动一个DataflowJob
     *
     * @param regCenter
     * @param jobEventConfig
     */
    private static void setUpDataflowJob(final CoordinatorRegistryCenter regCenter, final JobEventConfiguration jobEventConfig) {
        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder("javaDataflowElasticJob", "0/5 * * * * ?", 3).shardingItemParameters("0=Beijing,1=Shanghai,2=Guangzhou").build();
        DataflowJobConfiguration dataflowJobConfig = new DataflowJobConfiguration(coreConfig, JavaDataflowJob.class.getCanonicalName(), true);
        new JobScheduler(regCenter, LiteJobConfiguration.newBuilder(dataflowJobConfig).build(), jobEventConfig).init();
    }

    /**
     * 启动一个ScriptJob
     *
     * @param regCenter
     * @param jobEventConfig
     * @throws IOException
     */
    private static void setUpScriptJob(final CoordinatorRegistryCenter regCenter, final JobEventConfiguration jobEventConfig) throws IOException {
        JobCoreConfiguration coreConfig = JobCoreConfiguration.newBuilder("scriptElasticJob", "0/5 * * * * ?", 3).build();
        ScriptJobConfiguration scriptJobConfig = new ScriptJobConfiguration(coreConfig, buildScriptCommandLine());
        new JobScheduler(regCenter, LiteJobConfiguration.newBuilder(scriptJobConfig).build(), jobEventConfig).init();
    }
    
    private static String buildScriptCommandLine() throws IOException {
        if (System.getProperties().getProperty("os.name").contains("Windows")) {
            return Paths.get(JavaMain.class.getResource("/script/demo.bat").getPath().substring(1)).toString();
        }
        Path result = Paths.get(JavaMain.class.getResource("/script/demo.sh").getPath());
        Files.setPosixFilePermissions(result, PosixFilePermissions.fromString("rwxr-xr-x"));
        return result.toString();
    }
}
