package com.whz.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerListener;

public class MyTriggerListener implements TriggerListener {

    @Override
    public String getName() {
        return "MyOtherTriggerListener";
    }

    /**
     * 1、监听的job即将被运行时触发：当触发器触发并且与之关联的JobDetail将被执行时，由Scheduler调用.
     */
    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext context) {
        System.out.println("1、triggerFired()");
    }

    /**
     * 2、它关联的job即将被运行,先执行(1)，在执行(2) 如果返回TRUE 那么任务job会被终止
     */
    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
        System.out.println("2、vetoJobExecution()");
        return false;
    }

    /**
     * 3、当Trigger错过被触发时执行时，会调用该方法，比如当前时间有很多触发器都需要执行，但是线程池中的有效线程都在工作，那么有的触发器就有可能超时，错过这一轮的触发。
     */
    @Override
    public void triggerMisfired(Trigger trigger) {
        System.out.println("MyOtherTriggerListener.triggerMisfired()");
    }

    /**
     * 4、任务完成时触发
     */
    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext context,
            CompletedExecutionInstruction triggerInstructionCode) {
        System.out.println("4、triggerComplete()");
    }

}