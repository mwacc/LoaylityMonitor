package loaylitymonitor.servlets;

import loaylitymonitor.quartz.CronScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

public class InitializeServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(InitializeServlet.class);

    public void init() throws ServletException {

        try {
            System.out.println("Initializing Quartz scheduler");
            logger.info("Initializing Quartz scheduler");

            CronScheduler objPlugin = new CronScheduler("0 0/1 * * * ?");

        } catch (Exception ex) {
            logger.error("  JOB SCHEDULING WAS NOT STARTED!  ", ex);

        }

    }
}