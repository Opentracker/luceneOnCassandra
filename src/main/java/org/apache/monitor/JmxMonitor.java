package org.apache.monitor;

import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxMonitor {
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private MBeanServer mbs;
    private static JmxMonitor monitorInstance;
    private Map<String, OpentrackerClientMonitor> monitors;
    
    private JmxMonitor() {
        mbs = ManagementFactory.getPlatformMBeanServer();
        monitors = new HashMap<String, OpentrackerClientMonitor>();
    }
    
    public static JmxMonitor getInstance() {
        if (monitorInstance == null) {
            monitorInstance = new JmxMonitor();
        }
        return monitorInstance;
    }
    
    public void registerMonitor(String name, String monitorType, Object monitoringInterface) throws MalformedObjectNameException, NullPointerException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
        
        String monitorName = generateMonitorName(name, monitorType);
        logger.info("Registering JMX {}", monitorName);
        
        ObjectName oName = new ObjectName(monitorName);
        
        // check if the monitor is already registered
        if (mbs.isRegistered(oName)) {
            logger.info("Monitor already registered: {}", oName);
            return;
        }
        
        mbs.registerMBean(monitoringInterface, oName);
        
    }

    private String generateMonitorName(String className, String monitorType) {
        StringBuilder sb = new StringBuilder();
        sb.append(className.replaceAll(" ", "_"));
        sb.append(":ServiceType=");
        // append the classloader name so we have unique names in web apps.
        sb.append(getUniqueClassLoaderIdentifier());
        if (null != monitorType && monitorType.length() > 0) {
            sb.append(",MonitorType=" + monitorType);
        }
        
        return sb.toString();
    }

    private Object getUniqueClassLoaderIdentifier() {
        String contextPath = getContextPath();
        if (contextPath != null) {
            return contextPath.replaceAll(" ", "_");
        }
        return "Opentracker";
    }

    private String getContextPath() {
        ClassLoader loader = getClass().getClassLoader();
        if(loader == null)
         return null;
        URL url = loader.getResource("/");
        if (url != null) {
          String[] elements = url.toString().split("/");
          for (int i = elements.length - 1; i > 0; --i) {
            // URLs look like this: file:/.../ImageServer/WEB-INF/classes/
            // And we want that part that's just before WEB-INF
            if ("WEB-INF".equals(elements[i])) {
              return elements[i - 1];
            }
          }
        }
        return null;
    }
    
    public OpentrackerClientMonitor getCassandraMonitor(MonitorType monitorType) {
        OpentrackerClientMonitor opentrackerClientMonitor = monitors.get(monitorType.getMonitorName());
        if (opentrackerClientMonitor == null) {
            opentrackerClientMonitor = new OpentrackerClientMonitor(monitorType);
            try {
                registerMonitor("org.apache.service_"+monitorType.getMonitorName(), "opentracker", opentrackerClientMonitor);
            } catch (MalformedObjectNameException e) {
                logger.error("", e);
            } catch (InstanceAlreadyExistsException e) {
                logger.error("", e);
            } catch (MBeanRegistrationException e) {
                logger.error("", e);
            } catch (NotCompliantMBeanException e) {
                logger.error("", e);
            } catch (NullPointerException e) {
                logger.error("", e);
            }
            monitors.put(monitorType.getMonitorName(), opentrackerClientMonitor);
        }
        return opentrackerClientMonitor;
    }
    
    

}
