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

package com.dangdang.ddframe.job.lite.console.filter;

import com.dangdang.ddframe.job.lite.console.domain.EventTraceDataSourceConfiguration;
import com.dangdang.ddframe.job.lite.console.domain.EventTraceDataSourceFactory;
import com.dangdang.ddframe.job.lite.console.domain.RegistryCenterConfiguration;
import com.dangdang.ddframe.job.lite.console.service.EventTraceDataSourceConfigurationService;
import com.dangdang.ddframe.job.lite.console.service.RegistryCenterConfigurationService;
import com.dangdang.ddframe.job.lite.console.service.impl.EventTraceDataSourceConfigurationServiceImpl;
import com.dangdang.ddframe.job.lite.console.service.impl.RegistryCenterConfigurationServiceImpl;
import com.dangdang.ddframe.job.lite.console.util.SessionEventTraceDataSourceConfiguration;
import com.dangdang.ddframe.job.lite.console.util.SessionRegistryCenterConfiguration;
import com.dangdang.ddframe.job.lite.lifecycle.internal.reg.RegistryCenterFactory;
import com.dangdang.ddframe.job.reg.exception.RegException;
import com.google.common.base.Optional;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.io.IOException;

import static com.dangdang.ddframe.job.lite.console.restful.config.EventTraceDataSourceRestfulApi.DATA_SOURCE_CONFIG_KEY;
import static com.dangdang.ddframe.job.lite.console.restful.config.RegistryCenterRestfulApi.REG_CENTER_CONFIG_KEY;

/**
 * 全局配置过滤器.
 *
 * @author caohao
 */
public final class GlobalConfigurationFilter implements Filter {
    
    private final RegistryCenterConfigurationService regCenterService = new RegistryCenterConfigurationServiceImpl();
    
    private final EventTraceDataSourceConfigurationService rdbService = new EventTraceDataSourceConfigurationServiceImpl();
    
    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
    }
    
    @Override
    public void doFilter(final ServletRequest servletRequest, final ServletResponse servletResponse, final FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        HttpSession httpSession = httpRequest.getSession();
        // 加载注册中心
        if (null == httpSession.getAttribute(REG_CENTER_CONFIG_KEY)) {
            loadActivatedRegCenter(httpSession);
        }
        // 加载事件追踪数据源
        if (null == httpSession.getAttribute(DATA_SOURCE_CONFIG_KEY)) {
            loadActivatedEventTraceDataSource(httpSession);
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

    /**
     * 加载已连接的注册中心配置，并保存到session
     *
     * @param httpSession
     */
    private void loadActivatedRegCenter(final HttpSession httpSession) {
        // 从 ~/.elastic-job-console/Configurations.xml 读取已连接的注册中心配置
        Optional<RegistryCenterConfiguration> config = regCenterService.loadActivated();
        // isPresent() ：如果包含（非null）实例，则返回true。
        if (config.isPresent()) {
            String configName = config.get().getName();
            boolean isConnected = setRegistryCenterNameToSession(regCenterService.find(configName, regCenterService.loadAll()), httpSession);
            if (isConnected) {
                regCenterService.load(configName);
            }
        }
    }

    /**
     * 将注册中心配置保存到session
     *
     * @param regCenterConfig
     * @param session
     * @return
     */
    private boolean setRegistryCenterNameToSession(final RegistryCenterConfiguration regCenterConfig, final HttpSession session) {
        session.setAttribute(REG_CENTER_CONFIG_KEY, regCenterConfig);
        try {
            RegistryCenterFactory.createCoordinatorRegistryCenter(regCenterConfig.getZkAddressList(), regCenterConfig.getNamespace(), Optional.fromNullable(regCenterConfig.getDigest()));
            SessionRegistryCenterConfiguration.setRegistryCenterConfiguration((RegistryCenterConfiguration) session.getAttribute(REG_CENTER_CONFIG_KEY));
        } catch (final RegException ex) {
            return false;
        }
        return true;
    }
    
    private void loadActivatedEventTraceDataSource(final HttpSession httpSession) {
        Optional<EventTraceDataSourceConfiguration> config = rdbService.loadActivated();
        if (config.isPresent()) {
            String configName = config.get().getName();
            boolean isConnected = setEventTraceDataSourceNameToSession(rdbService.find(configName, rdbService.loadAll()), httpSession);
            if (isConnected) {
                rdbService.load(configName);
            }
        }
    }
    
    private boolean setEventTraceDataSourceNameToSession(final EventTraceDataSourceConfiguration dataSourceConfig, final HttpSession session) {
        session.setAttribute(DATA_SOURCE_CONFIG_KEY, dataSourceConfig);
        try {
            EventTraceDataSourceFactory.createEventTraceDataSource(dataSourceConfig.getDriver(), dataSourceConfig.getUrl(),
                    dataSourceConfig.getUsername(), Optional.fromNullable(dataSourceConfig.getPassword()));
            SessionEventTraceDataSourceConfiguration.setDataSourceConfiguration((EventTraceDataSourceConfiguration) session.getAttribute(DATA_SOURCE_CONFIG_KEY));
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            return false;
        }
        return true;
    }
    
    @Override
    public void destroy() {
    }
}
