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

package com.dangdang.ddframe.job.lite.console.service.impl;

import com.dangdang.ddframe.job.lite.console.domain.GlobalConfiguration;
import com.dangdang.ddframe.job.lite.console.domain.RegistryCenterConfiguration;
import com.dangdang.ddframe.job.lite.console.domain.RegistryCenterConfigurations;
import com.dangdang.ddframe.job.lite.console.repository.ConfigurationsXmlRepository;
import com.dangdang.ddframe.job.lite.console.repository.impl.ConfigurationsXmlRepositoryImpl;
import com.dangdang.ddframe.job.lite.console.service.RegistryCenterConfigurationService;
import com.google.common.base.Optional;

/**
 * 注册中心配置服务实现类.
 * 配置保存在 ~/.elastic-job-console/Configurations.xml 配置文件中，该服务主要对该配置文件进行增删改查操作
 *
 * @author zhangliang
 */
public final class RegistryCenterConfigurationServiceImpl implements RegistryCenterConfigurationService {

    /** 对应 ~/.elastic-job-console/Configurations.xml 配置文件 */
    private ConfigurationsXmlRepository configurationsXmlRepository = new ConfigurationsXmlRepositoryImpl();

    /**
     * 从全局配置获取注册中心部分的配置
     *
     * @return
     */
    @Override
    public RegistryCenterConfigurations loadAll() {
        return loadGlobal().getRegistryCenterConfigurations();
    }

    /**
     * 加载已激活的注册中心配置
     *
     * @param name 配置名称
     * @return
     */
    @Override
    public RegistryCenterConfiguration load(final String name) {
        // 加载全局配置
        GlobalConfiguration configs = loadGlobal();
        // 从configs中获取对应的配置名的配置
        RegistryCenterConfiguration result = find(name, configs.getRegistryCenterConfigurations());
        // 将已连接的注册中心设置为激活状态，并将其他注册中心设置为非激活状态
        setActivated(configs, result);
        return result;
    }

    /**
     * 从configs中获取对应的配置名的配置
     *
     * @param name 配置名称
     * @param configs 全部注册中心配置
     * @return
     */
    @Override
    public RegistryCenterConfiguration find(final String name, final RegistryCenterConfigurations configs) {
        for (RegistryCenterConfiguration each : configs.getRegistryCenterConfiguration()) {
            if (name.equals(each.getName())) {
                return each;
            }
        }
        return null;
    }

    /**
     * 将已连接的注册中心设置为激活状态，并将其他注册中心设置为非激活状态
     *
     * @param configs               对应~/.elastic-job-console/Configurations.xml配置文件
     * @param toBeConnectedConfig
     */
    private void setActivated(final GlobalConfiguration configs, final RegistryCenterConfiguration toBeConnectedConfig) {
        // 获取所有已连接的注册中心配置
        RegistryCenterConfiguration activatedConfig = findActivatedRegistryCenterConfiguration(configs);
        if (!toBeConnectedConfig.equals(activatedConfig)) {
            if (null != activatedConfig) {
                activatedConfig.setActivated(false);
            }
            toBeConnectedConfig.setActivated(true);
            configurationsXmlRepository.save(configs);
        }
    }

    /**
     * 从 ~/.elastic-job-console/Configurations.xml 读取已连接的注册中心配置
     *
     * @return
     */
    @Override
    public Optional<RegistryCenterConfiguration> loadActivated() {
        return Optional.fromNullable(findActivatedRegistryCenterConfiguration(loadGlobal()));
    }

    /**
     * 获取已连接的注册中心配置
     *
     * @param configs
     * @return
     */
    private RegistryCenterConfiguration findActivatedRegistryCenterConfiguration(final GlobalConfiguration configs) {
        for (RegistryCenterConfiguration each : configs.getRegistryCenterConfigurations().getRegistryCenterConfiguration()) {
            if (each.isActivated()) {
                return each;
            }
        }
        return null;
    }

    /**
     * 添加配置
     *
     * @param config 注册中心配置
     * @return
     */
    @Override
    public boolean add(final RegistryCenterConfiguration config) {
        GlobalConfiguration configs = loadGlobal();
        boolean result = configs.getRegistryCenterConfigurations().getRegistryCenterConfiguration().add(config);
        if (result) {
            configurationsXmlRepository.save(configs);
        }
        return result;
    }

    /**
     * 删除配置
     *
     * @param name 配置名称
     */
    @Override
    public void delete(final String name) {
        GlobalConfiguration configs = loadGlobal();
        RegistryCenterConfiguration toBeRemovedConfig = find(name, configs.getRegistryCenterConfigurations());
        if (null != toBeRemovedConfig) {
            configs.getRegistryCenterConfigurations().getRegistryCenterConfiguration().remove(toBeRemovedConfig);
            configurationsXmlRepository.save(configs);
        }
    }

    /**
     * 解析~/.elastic-job-console/Configurations.xml配置文件，返回配置对象 GlobalConfiguration
     *
     * @return
     */
    private GlobalConfiguration loadGlobal() {
        GlobalConfiguration result = configurationsXmlRepository.load();
        if (null == result.getRegistryCenterConfigurations()) {
            result.setRegistryCenterConfigurations(new RegistryCenterConfigurations());
        }
        return result;
    }
}
