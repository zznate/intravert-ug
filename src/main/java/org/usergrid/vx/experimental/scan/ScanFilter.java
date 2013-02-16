package org.usergrid.vx.experimental.scan;
import java.util.List;
import java.util.Map;

public interface ScanFilter {
    public boolean filter(Map m, ScanContext contex);
}
