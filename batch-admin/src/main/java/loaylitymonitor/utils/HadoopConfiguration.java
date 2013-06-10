package loaylitymonitor.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HadoopConfiguration {

    private final Configuration conf;

    public HadoopConfiguration() {
        conf = new Configuration();
        conf.addResource(new Path("/share/hadoop-1.0.3/conf/core-site.xml"));
        conf.addResource(new Path("/share/hadoop-1.0.3/conf/hdfs-site.xml"));
    }

    public Configuration getConf() {
        return conf;
    }
}