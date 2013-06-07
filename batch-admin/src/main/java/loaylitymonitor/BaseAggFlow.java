package loaylitymonitor;

import loaylitymonitor.pig.PigExecutor;
import loaylitymonitor.utils.FileSystemUtils;
import loaylitymonitor.utils.HadoopConfiguration;

import java.io.IOException;

public class BaseAggFlow {

    /**
     * 1. llokup for available data
     * 2. run pig aggregation
     * 3. push result to mysql
     * 4. delete already processed dirs
     */
    public static void main(String[] args) throws IOException {
        HadoopConfiguration hdpCfg = new HadoopConfiguration();
        FileSystemUtils fs = new FileSystemUtils(hdpCfg);
        PigExecutor pigExecutor = new PigExecutor( args[0] );

        if( fs.isReadyDataForProcessing() && pigExecutor.runBaseAggPigScript() ) {
            fs.removeProcessedDirs();
            System.out.println("Sucess!!!!");
        }
    }

}