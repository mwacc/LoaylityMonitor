package loaylitymonitor.quartz;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CronScheduler {

    public CronScheduler(String cronExpression) throws Exception {

        SchedulerFactory sf = new StdSchedulerFactory();

        Scheduler sche = sf.getScheduler();

        sche.start();

        JobDetail jDetail = newJob(RunBaseAggregationJob.class)
                .withIdentity("job1", "group1")
                .build();

        //"0 0 12 * * ?" Fire at 12pm (noon) every day
        //"0/2 * * * * ?" Fire at every 2 seconds every day

        CronTrigger crTrigger = newTrigger()
                .withIdentity("trigger1", "group1")
                .withSchedule(cronSchedule( cronExpression ))
                .forJob("job1", "group1")
                .build();

        sche.scheduleJob(jDetail, crTrigger);
    }


}