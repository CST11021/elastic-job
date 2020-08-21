package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.executor.AbstractElasticJobExecutor;
import com.dangdang.ddframe.job.executor.JobExecutorFactory;
import com.dangdang.ddframe.job.executor.JobFacade;
import lombok.Setter;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Lite调度作业，基于quartz框架实现，这里实现了org.quartz.Job接口
 *
 * @author zhangliang
 */
public final class LiteJob implements Job {
    
    @Setter
    private ElasticJob elasticJob;
    
    @Setter
    private JobFacade jobFacade;

    /**
     * 执行作业
     *
     * @param context
     * @throws JobExecutionException
     */
    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException {
        AbstractElasticJobExecutor executor = JobExecutorFactory.getJobExecutor(elasticJob, jobFacade);
        executor.execute();
    }
}
