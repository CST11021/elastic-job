package com.whz.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Quartz定时执行事件
 * 作者 林炳文（ling20081005@126.com 博客：http://blog.csdn.net/evankaka）
 * 时间 2015.4.29
 */
public class QuartzTest implements Job {


    public static void main(String[] args) {
        QuartzTest quartzTest = new QuartzTest();
        quartzTest.startSchedule();
    }

    /**
     * 事件类，处理具体的业务
     */
    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        System.out.println("Hello quzrtz  " +
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ").format(new Date()));
    }

    public void startSchedule() {
        try {

            // 1、创建一个JobDetail实例，指定Quartz
            JobDetail jobDetail = JobBuilder.newJob(QuartzTest.class)
                    .build();

            // 2、创建Trigger
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger1", "group1")
                    .startNow()
                    .withSchedule(
                            SimpleScheduleBuilder.simpleSchedule()
                                    // 设置间隔执行时间
                                    .withIntervalInSeconds(5)
                                    // 设置执行次数
                                    .repeatForever()
                    )
                    .build();

            // 3、创建Scheduler
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.getListenerManager().addTriggerListener(new MyTriggerListener());
            scheduler.start();

            //4、调度执行
            scheduler.scheduleJob(jobDetail, trigger);

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            scheduler.shutdown();

        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }


}