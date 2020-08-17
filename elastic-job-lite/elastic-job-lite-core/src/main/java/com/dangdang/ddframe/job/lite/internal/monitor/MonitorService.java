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

package com.dangdang.ddframe.job.lite.internal.monitor;

import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.util.SensitiveInfoUtils;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * 作业监控服务.
 * 
 * @author caohao
 */
@Slf4j
public final class MonitorService {

    /** 表示来自客户端的dump命令 */
    public static final String DUMP_COMMAND = "dump";

    /** 作业名称 */
    private final String jobName;

    /** zk客户端 */
    private final CoordinatorRegistryCenter regCenter;

    /** 用于从注册中心读取 LiteJobConfiguration 配置，和注册作业配置到注册中心 */
    private final ConfigurationService configService;

    /** 表示一个服务端的socket监听服务，用于接收来自客户端的请求 */
    private ServerSocket serverSocket;

    /** 用于标记该作业监听服务MonitorService是否已经关闭 */
    private volatile boolean closed;
    
    public MonitorService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        this.regCenter = regCenter;
        configService = new ConfigurationService(regCenter, jobName);
    }
    
    /**
     * 初始化作业监听服务：从配置中获取监听端口，并开启socket监听，接收来自客户端的请求
     */
    public void listen() {
        int port = configService.load(true).getMonitorPort();
        if (port < 0) {
            return;
        }

        try {
            log.info("Elastic job: Monitor service is running, the port is '{}'", port);
            openSocketForMonitor(port);
        } catch (final IOException ex) {
            log.error("Elastic job: Monitor service listen failure, error is: ", ex);
        }
    }

    /**
     * 开启socket监听，接收来自客户端的请求
     *
     * @param port
     * @throws IOException
     */
    private void openSocketForMonitor(final int port) throws IOException {
        serverSocket = new ServerSocket(port);
        new Thread() {
            
            @Override
            public void run() {
                while (!closed) {
                    try {
                        process(serverSocket.accept());
                    } catch (final IOException ex) {
                        log.error("Elastic job: Monitor service open socket for monitor failure, error is: ", ex);
                    }
                }
            }
        }.start();
    }

    /**
     * 处理客户端请求
     *
     * @param socket
     * @throws IOException
     */
    private void process(final Socket socket) throws IOException {
        try (
                // 获取客户端的请求数据
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                // 用于响应客户端的通道
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                Socket autoCloseSocket = socket
        ) {
            // 获取客户端请求的命令，目前仅支持dump命令
            String cmdLine = reader.readLine();
            if (null != cmdLine && DUMP_COMMAND.equalsIgnoreCase(cmdLine)) {
                List<String> result = new ArrayList<>();
                // 递归dump出作业节点（/${jobName}）下所有配置信息，并保存在result
                dumpDirectly("/" + jobName, result);
                // 将dump信息发送给客户端
                outputMessage(writer, Joiner.on("\n").join(SensitiveInfoUtils.filterSensitiveIps(result)) + "\n");
            }
        }
    }

    /**
     * 递归dump出作业节点（/${jobName}）下所有配置信息，并保存在result
     *
     * @param path      作业路径，例如：/${jobName}
     * @param result
     */
    private void dumpDirectly(final String path, final List<String> result) {
        // 获取作业根目录下所有子节点，包括：config、instances、servers、sharding、leader
        for (String each : regCenter.getChildrenKeys(path)) {
            // 获取子节点路径
            String zkPath = path + "/" + each;
            // 获取子节点数据
            String zkValue = regCenter.get(zkPath);
            if (null == zkValue) {
                zkValue = "";
            }

            TreeCache treeCache = (TreeCache) regCenter.getRawCache("/" + jobName);
            ChildData treeCacheData = treeCache.getCurrentData(zkPath);
            String treeCachePath =  null == treeCacheData ? "" : treeCacheData.getPath();
            String treeCacheValue = null == treeCacheData ? "" : new String(treeCacheData.getData());
            if (zkValue.equals(treeCacheValue) && zkPath.equals(treeCachePath)) {
                result.add(Joiner.on(" | ").join(zkPath, zkValue));
            } else {
                result.add(Joiner.on(" | ").join(zkPath, zkValue, treeCachePath, treeCacheValue));
            }
            dumpDirectly(zkPath, result);
        }
    }
    
    private void outputMessage(final BufferedWriter outputWriter, final String msg) throws IOException {
        outputWriter.append(msg);
        outputWriter.flush();
    }
    
    /**
     * 关闭作业监听服务.
     */
    public void close() {
        closed = true;
        if (null != serverSocket && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (final IOException ex) {
                log.error("Elastic job: Monitor service close failure, error is: ", ex);
            }
        }
    }
}
