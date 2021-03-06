package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.cassandra.CassandraClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraClient extends OpentrackerTestBase {
    
    private CassandraClient cc = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        try {
            cc = new CassandraClient(cassandraHost, rpcPort, frameMode, keyspace, columnFamily, blockSize);
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @After
    public void tearDown() throws Exception {
        cc.close();
    }

    @Test
    public void testCassandraClient() {
        assertNotNull(cc);
    }

    @Test
    public void testGetKeys() {
        List<byte[]> columnNames = new ArrayList<byte[]>();
        columnNames.add("DESCRIPTOR".getBytes());        
        
        try {
            
            ByteBuffer insertKey = ByteBufferUtil.bytes("sampleFile");
            Map<byte[], byte[]> columns = new LinkedHashMap<byte[], byte[]>();
            columns.put("DESCRIPTOR".getBytes(), "{\"lastModified\":1397557341307,\"name\":\"/DESCRIPTOR\",\"length\":0,\"blocks\":[],\"deleted\":false,\"lastAccessed\":1397557341307}".getBytes());

            cc.setColumns(insertKey, columns);
            
            byte[][] keys = cc.getKeys(columnNames, 131072, false);
            
            for (byte[] key : keys) {
                System.out.println(new String(key));
            }
            
            assertTrue(keys.length > 0);
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @Test
    public void testGetAllKeys() {
        List<byte[]> columnNames = new ArrayList<byte[]>();
        columnNames.add("DESCRIPTOR".getBytes());

        try {

            cc.truncate(columnFamily);

            ByteBuffer insertKey = ByteBufferUtil.bytes("sampleFile");
            Map<byte[], byte[]> columns = new LinkedHashMap<byte[], byte[]>();
            columns.put("DESCRIPTOR".getBytes(), "{\"lastModified\":1397557341307,\"name\":\"/DESCRIPTOR\",\"length\":0,\"blocks\":[],\"deleted\":false,\"lastAccessed\":1397557341307}".getBytes());

            cc.setColumns(insertKey, columns);

            insertKey = ByteBufferUtil.bytes("sampleFile1");
            columns.put("DESCRIPTOR".getBytes(), "{\"lastModified\":1397557341307,\"name\":\"/DESCRIPTOR\",\"length\":0,\"blocks\":[],\"deleted\":true,\"lastAccessed\":1397557341307}".getBytes());
            cc.setColumns(insertKey, columns);

            byte[][] keys = cc.getKeys(columnNames, 131072, true);
            ArrayList<String> actuals = new ArrayList<String>();

            for (byte[] key : keys) {
                actuals.add(new String(key));
            }
            assertTrue(actuals.contains("sampleFile"));
            assertTrue(actuals.contains("sampleFile1"));
            assertEquals(2, keys.length);

        } catch (Exception e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @Test
    public void testGetColumns() {
        byte[] key = "sampleFile".getBytes();
        Set<byte[]> columns = new LinkedHashSet<byte[]>();  
        columns.add("DESCRIPTOR".getBytes());
        
        try {
            Map<byte[], byte[]> results = cc.getColumns(key, columns);
            for (Entry<byte[], byte[]> data : results.entrySet()) {
                System.out.println("key " + new String(data.getKey()));
                System.out.println("value " + new String(data.getValue()));
            }
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @Test
    public void testGetColumn() {
        
        ByteBuffer insertKey = ByteBufferUtil.bytes("sampleFile");
        Map<byte[], byte[]> columns = new LinkedHashMap<byte[], byte[]>();
        columns.put("DESCRIPTOR".getBytes(), "descriptor value".getBytes());

        
        byte[] key = "sampleFile".getBytes();
        byte[] columnName = "DESCRIPTOR".getBytes();
        
        
        try {
            cc.setColumns(insertKey, columns);
            
            byte[] result = cc.getColumn(key, columnName);
            System.out.println(new String(result));
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @Test
    public void testSetColumns() {
        ByteBuffer key = ByteBufferUtil.bytes("data2");
        Map<byte[], byte[]> columns = new LinkedHashMap<byte[], byte[]>();
        columns.put("column1".getBytes(), "column1 value".getBytes());
        
        try {
            cc.setColumns(key, columns);
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

    @Test
    public void testTruncate() {
        try {
            cc.truncate("index1");
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
    }

}
