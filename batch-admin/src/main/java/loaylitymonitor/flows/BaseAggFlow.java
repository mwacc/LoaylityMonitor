package loaylitymonitor.flows;

import loaylitymonitor.cmd.SqoopRunner;
import loaylitymonitor.pig.PigExecutor;
import loaylitymonitor.utils.FileSystemUtils;
import loaylitymonitor.utils.HadoopConfiguration;

import java.io.IOException;

public class BaseAggFlow extends BaseFlow {

    public BaseAggFlow() throws IOException {
        super();
    }

    protected void runFlow() throws IOException {
        if( fs.isReadyDataForProcessing() ) {
            sqoop.runExport(pigExecutor.runBaseAggPigScript(), "sentiments_facts", "created_at","campaignid,messagenum","sentiments" );
            fs.removeProcessedDirs();
            System.out.println("Sucess!!!!");
        }
    }

    /**
     * 1. llokup for available data
     * 2. run pig aggregation
     * 3. push result to mysql
     * 4. delete already processed dirs
     */
    public static void main(String[] args) throws IOException {
        try {
            new BaseAggFlow().runFlow();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

}