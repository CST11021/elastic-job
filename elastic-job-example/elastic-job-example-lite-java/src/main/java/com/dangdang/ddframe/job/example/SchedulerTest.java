package com.dangdang.ddframe.job.example;

import com.dangdang.ddframe.job.lite.internal.schedule.JobShutdownHookPlugin;
import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.listeners.TriggerListenerSupport;

import java.util.Properties;

/**
 * @Author: wanghz
 * @Date: 2020/4/22 8:17 PM
 */
public class SchedulerTest {

    public static void main(String[] args) throws SchedulerException {

        StdSchedulerFactory factory = new StdSchedulerFactory();
        factory.initialize(getBaseQuartzProperties());

        Scheduler result = factory.getScheduler();
        result.getListenerManager().addTriggerListener(new TestListener());

    }

    /**
     * 获取quartz调度器配置
     *
     * @return
     */
    private static Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", "testJob");
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }

    public static class TestListener extends TriggerListenerSupport {

        @Override
        public String getName() {
            return "JobTriggerListener";
        }

        @Override
        public void triggerMisfired(final Trigger trigger) {
            System.out.println("trigger：");
        }

    }

}
