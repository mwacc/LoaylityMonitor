package loaylitymonitor.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.launch.support.CommandLineJobRunner;

import java.io.File;

public class RunBaseAggregationJob implements Job {
    private static Logger logger = LoggerFactory.getLogger(RunBaseAggregationJob.class);

    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        System.out.println("Start cron job");
        logger.info("Cron start executing ");
        try {
            // TODO: run spring batch job
            CommandLineJobRunner.main(new String[]{"classpath:/spring/spring-context.xml", "baseAggJob"});
        } catch (Exception e) {
            logger.error("Can't run spring batch 'baseAggJob' job" + e);
        }

    }
}