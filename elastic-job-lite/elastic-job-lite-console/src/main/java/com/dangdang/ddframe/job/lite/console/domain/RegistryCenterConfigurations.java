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

package com.dangdang.ddframe.job.lite.console.domain;

import lombok.Getter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 注册中心配置根对象.
 * 对应~/.elastic-job-console/Configurations.xml配置文件注册中心配置节点
 * <globalConfiguration>
 *     <registryCenterConfigurations>
 *         ...
 *     </registryCenterConfigurations>
 * </globalConfiguration>
 *
 * @author zhangliang
 */
@Getter
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public final class RegistryCenterConfigurations {
    
    private Set<RegistryCenterConfiguration> registryCenterConfiguration = new LinkedHashSet<>();
}
