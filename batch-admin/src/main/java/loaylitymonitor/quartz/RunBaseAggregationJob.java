package loaylitymonitor.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.converter.JobParametersConverter;
import org.springframework.batch.core.launch.support.CommandLineJobRunner;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RunBaseAggregationJob implements Job {
    private static Logger logger = LoggerFactory.getLogger(RunBaseAggregationJob.class);

    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        System.out.println("Start cron job");
        logger.info("Cron start executing ");
        try {
            // TODO: run spring batch job
            CommandLineJobRunner.main(new String[]{"classpath:/spring/spring-context.xml", "baseAggJob", "startAt="+String.valueOf(System.currentTimeMillis()) });
        } catch (Exception e) {
            logger.error("Can't run spring batch 'baseAggJob' job" + e);
        }

    }

}