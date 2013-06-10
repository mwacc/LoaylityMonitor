package loaylitymonitor.flows;

import loaylitymonitor.cmd.SqoopRunner;
import loaylitymonitor.pig.PigExecutor;
import loaylitymonitor.utils.FileSystemUtils;
import loaylitymonitor.utils.HadoopConfiguration;

import java.io.IOException;

public abstract class BaseFlow {

    protected final HadoopConfiguration hdpCfg;
    protected final FileSystemUtils fs;
    protected final PigExecutor pigExecutor;
    protected final SqoopRunner sqoop;

    protected BaseFlow() throws IOException {
        hdpCfg = new HadoopConfiguration();
        fs = new FileSystemUtils(hdpCfg);
        pigExecutor = new PigExecutor( );
        sqoop = new SqoopRunner();
    }

    protected abstract void runFlow() throws IOException;
}