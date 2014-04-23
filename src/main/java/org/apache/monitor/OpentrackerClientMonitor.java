package org.apache.monitor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpentrackerClientMonitor implements OpentrackerClientMonitorMBean {
    
    private static Logger logger = LoggerFactory.getLogger(OpentrackerClientMonitor.class);
    
    private final Map<Counter, AtomicLong> counters;
    private final MonitorType monitorType;
    

    public OpentrackerClientMonitor(MonitorType monitorType) {
        logger.info("initializing monitor {}", monitorType.getMonitorName());
        this.monitorType = monitorType;
        counters = this.monitorType.getCounters();
    }
    
    public void incCounter(Counter counterType) {
        counters.get(counterType).incrementAndGet();
    }
    
    public void addCounter(Counter counterType, long value) {
        counters.get(counterType).addAndGet(value);
    }
    
    public void setCounter(Counter counterType, long value) {
        counters.get(counterType).set(value);
    }
    

    @Override
    public long getMetricModeMerge() {
        return counters.get(Counter.METRIC_MODE_MERGE).longValue();
    }

    @Override
    public double getTotalMetricModeMerge() {
        return counters.get(Counter.METRIC_TOTAL_MODE_MERGE).doubleValue();
    }

}
