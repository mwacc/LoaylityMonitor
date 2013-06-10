package loaylitymonitor.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemUtils {

    private final FileSystem fileSystem;

    public FileSystemUtils(HadoopConfiguration hCfg) throws IOException {
        fileSystem = FileSystem.get(hCfg.getConf());
    }

    public boolean isReadyDataForProcessing() throws IOException {
        // Check if the file already exists
        Path path = new Path("/loyality/flume/tweets");
        if( isPathExist(path) ) {
            return prepareDataFromPigFlow(path);
        }
        return false;
    }

    public boolean removeProcessedDirs() throws IOException {
        Path path = new Path("/loyality/working");
        if( isPathExist(path) ) {
            for(FileStatus f : fileSystem.listStatus(path)) {
                fileSystem.delete(f.getPath(), true);
            }
            return true;
        }
        return false;
    }

    public boolean isPathExist(Path path) throws IOException {
        if (!fileSystem.exists(path)) {
            System.err.println( String.format("Path %s doesn't exist!", path) );
            return false;
        }

        return true;
    }

    private boolean prepareDataFromPigFlow(Path path) throws IOException {
        for(FileStatus f : fileSystem.listStatus(path)) {
            if( !f.isDir() ) {
                if( !f.getPath().getName().endsWith(".tmp") ) { // end w/ .tmp
                    fileSystem.rename(f.getPath(), new Path("/loyality/working"+f.getPath().getName()));
                }
            } else {
                prepareDataFromPigFlow(f.getPath());
            }
        }
        return true;
    }

}