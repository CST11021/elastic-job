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

package com.dangdang.ddframe.job.lite.console.repository.impl;

import com.dangdang.ddframe.job.lite.console.exception.JobConsoleException;
import com.dangdang.ddframe.job.lite.console.repository.XmlRepository;
import com.dangdang.ddframe.job.lite.console.util.HomeFolderUtils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.File;

/**
 * 基于XML的数据访问器实现类.
 * 
 * @param <E> 数据类型
 * @author zhangliang
 */
public abstract class AbstractXmlRepositoryImpl<E> implements XmlRepository<E> {

    /** 对应 ~/.elastic-job-console/Configurations.xml 配置文件 */
    private final File file;
    /** 对应 GlobalConfiguration 配置类 */
    private final Class<E> clazz;
    
    private JAXBContext jaxbContext;

    /**
     *
     * @param fileName  配置文件名，例如：Configurations.xml
     * @param clazz     配置文件对应的配置类
     */
    protected AbstractXmlRepositoryImpl(final String fileName, final Class<E> clazz) {
        file = new File(HomeFolderUtils.getFilePathInHomeFolder(fileName));
        this.clazz = clazz;
        HomeFolderUtils.createHomeFolderIfNotExisted();

        try {
            jaxbContext = JAXBContext.newInstance(clazz);
        } catch (final JAXBException ex) {
            throw new JobConsoleException(ex);
        }
    }

    /**
     * 加载配置文件Configurations.xml，返回配置对象GlobalConfiguration
     *
     * @return
     */
    @Override
    public synchronized E load() {
        if (!file.exists()) {
            try {
                return clazz.newInstance();
            } catch (final InstantiationException | IllegalAccessException ex) {
                throw new JobConsoleException(ex);
            }
        }

        try {
            @SuppressWarnings("unchecked")
            E result = (E) jaxbContext.createUnmarshaller().unmarshal(file);
            return result;
        } catch (final JAXBException ex) {
            throw new JobConsoleException(ex);
        }
    }

    /**
     * 向配置文件添加配置
     *
     * @param entity 数据
     */
    @Override
    public synchronized void save(final E entity) {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            marshaller.marshal(entity, file);
        } catch (final JAXBException ex) {
            throw new JobConsoleException(ex);
        }
    }
}
