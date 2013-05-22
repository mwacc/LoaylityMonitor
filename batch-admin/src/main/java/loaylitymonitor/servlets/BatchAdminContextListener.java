package loaylitymonitor.servlets;

import loaylitymonitor.quartz.CronScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import javax.servlet.http.HttpSessionBindingEvent;

public class BatchAdminContextListener implements ServletContextListener{
    private static Logger logger = LoggerFactory.getLogger(BatchAdminContextListener.class);
    private CronScheduler cronScheduler;

    // Public constructor is required by servlet spec
    public BatchAdminContextListener() {
    }

    // -------------------------------------------------------
    // ServletContextListener implementation
    // -------------------------------------------------------
    public void contextInitialized(ServletContextEvent sce) {
        try {
            String springConfig = sce.getServletContext().getInitParameter("param");
            System.out.println("Initializing Quartz scheduler");
            logger.info("Initializing Quartz scheduler");

            cronScheduler = new CronScheduler("0 0/1 * * * ?");

        } catch (Exception ex) {
            logger.error("  JOB SCHEDULING WAS NOT STARTED!  ", ex);

        }
    }

    public void contextDestroyed(ServletContextEvent sce) {
    }


}
