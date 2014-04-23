package org.apache.hector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.connection.ConcurrentHClientPool;
import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.connection.client.HClient;
import me.prettyprint.cassandra.connection.factory.HClientFactory;
import me.prettyprint.cassandra.connection.factory.HThriftClientFactoryImpl;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.KeyspaceService;
import me.prettyprint.cassandra.service.KeyspaceServiceImpl;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;

public class OpentrackerHector {
    
    private static Logger logger = LoggerFactory.getLogger(OpentrackerHector.class);
    
    private CassandraHostConfigurator cassandraHostConfigurator;
    private HConnectionManager connectionManager;
    private Keyspace keyspace;
    private KeyspaceService keyspaceService;
    
    private CassandraHost cassandraHost;
    private ConcurrentHClientPool clientPool;
    
    public String clusterName = "Test Cluster";
    Cluster cluster = null;
    
    public OpentrackerHector() {
        cassandraHostConfigurator = new CassandraHostConfigurator();
        cassandraHostConfigurator.setHosts("127.0.0.1:9160,gl04.opentracker.net:9160");
        
        connectionManager = new HConnectionManager(clusterName, cassandraHostConfigurator);
        
        cassandraHost = cassandraHostConfigurator.buildCassandraHosts()[0];
        HClientFactory factory = new HThriftClientFactoryImpl();
        clientPool = new ConcurrentHClientPool(factory, cassandraHost);
        
        cluster = getOrCreateCluster(clusterName, cassandraHostConfigurator);
    }
    
    public String getPoolStats() {
        return clientPool.getNumIdle() +  " " + clientPool.getNumBeforeExhausted() + " " + clientPool.getNumBlockedThreads() + " " + clientPool.getNumActive();
    }
    
    public boolean shutdownPool() {
        try {
            clientPool.shutdown();
            return true;
        } catch (Exception e) {
            logger.error("unable to shutdown pool ", e);
        }
        return false;
    }
    
    public HClient getConnection() {
        HClient client = clientPool.borrowClient();
        return client;
    }
    
    public boolean releaseConnection(HClient client) {
        try {
            clientPool.releaseClient(client);
            return true;
        } catch (Exception e) {
            logger.error("unable to release client " + client.getCassandraHost().getName(), e);
        }
        return false;
    }
    
    public Keyspace getKeyspace(String keyspace) {
        this.keyspace = createKeyspace(keyspace, cluster);
        return this.keyspace;
    }
    
    // not good, cannot delete row.
    public KeyspaceService getKeyspaceService(String keyspace) {
        keyspaceService = new KeyspaceServiceImpl(keyspace, new AllOneConsistencyLevelPolicy(), connectionManager, FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE);
        return keyspaceService;
    }

}