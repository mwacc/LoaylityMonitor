package loaylitymonitor.utils;

import org.springframework.batch.core.launch.support.SystemExiter;

public class MockSystemExiter implements SystemExiter {
    @Override
    public void exit(int i) {
        // do nothing
        System.out.print("Exit w/ status "+i);
    }
}