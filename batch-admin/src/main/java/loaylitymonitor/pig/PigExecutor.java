package loaylitymonitor.pig;

import loaylitymonitor.utils.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PigExecutor {

    private final PigServer pigServer;
    private final String pathToPig;

    public PigExecutor(String pathToPig) throws IOException {
        Properties props = new Properties();
        props.setProperty("fs.default.name", "hdfs://localhost:9000");
        props.setProperty("mapred.job.tracker", "localhost:9001");


        this.pigServer = new PigServer(ExecType.MAPREDUCE, props);
        this.pathToPig = pathToPig;
    }

    public boolean runScript(String scriptName, Map<String, String> params) throws IOException {
        boolean isFailed = false;

        pigServer.setBatchOn();
        pigServer.registerScript(scriptName, params);
        List<ExecJob> jobs = pigServer.executeBatch();

        for(ExecJob job : jobs) {
            if( ExecJob.JOB_STATUS.FAILED == job.getStatus() ) {
                isFailed = true;
            }
        }
        return !isFailed;
    }

    public boolean runBaseAggPigScript() throws IOException {
        Map<String, String> params = new HashMap();
        params.put("input", "/loyality/working");
        params.put("output", "/loyality/agg/" + DateUtils.getFormdatedDate(new Date()));
        params.put("parallel", "2");
        params.put("pathToElephantBird", String.format("%s/jars/elephant-bird-pig-3.0.0.jar", pathToPig));
        params.put("pathToSimpleJson", String.format("%s/jars/json-simple-1.1.1.jar", pathToPig));
        params.put("pathToCustomLib", String.format("%s/jars/custom-pig-1.0-SNAPSHOT.jar", pathToPig));

        return runScript(String.format("%s/pig/baseAggJob.pig", pathToPig), params);
    }

}