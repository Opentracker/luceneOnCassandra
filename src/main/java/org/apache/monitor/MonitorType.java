package org.apache.monitor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public interface MonitorType {

    public String getMonitorName();
    
    /**
     * ALWAYS and MUST register metric you want to instrument into the class 
     * <code>Counter</code>.
     * @return
     */
    public  Map<Counter, AtomicLong> getCounters();

}
