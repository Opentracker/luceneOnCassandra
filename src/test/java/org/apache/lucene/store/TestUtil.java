package org.apache.lucene.store;

import static org.junit.Assert.*;

import org.apache.lucene.cassandra.CassandraFile;
import org.apache.lucene.cassandra.Util;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUtil {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testBytesToHex() {
        byte[] b = "number".getBytes();
        assertEquals("6E756D626572",Util.bytesToHex(b));
    }

    @Test
    public void testDebugBytesToHex() {
        byte[] b = "this is a hello world long long example.".getBytes();
        assertEquals("7468697320...6D706C652E", Util.debugBytesToHex(b));
    }

    @Test
    public void testHexToAscii() {
        assertEquals("_1.cfs" ,Util.hexToAscii("5f312e636673"));
    }
    
    @Test
    public void testGetFileName() {
        String indexPath = "index1";
        String keyspace = "lucene1";
        String columnFamily = "index1";
        int blockSize  = 16384;
        String cassandraDirectory = "/www.exchangerates.org.uk/2014-04-15/0000/";
        CassandraFile file = new CassandraFile(cassandraDirectory, indexPath, IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        
        assertEquals(indexPath, Util.getFileName(file));
    }
    
    @Test
    public void testGetCassandraPath() {
        String indexPath = "index1";
        String keyspace = "lucene1";
        String columnFamily = "index1";
        int blockSize  = 16384;
        String cassandraDirectory = "/www.exchangerates.org.uk/2014-04-15/0000/";
        CassandraFile file = new CassandraFile(cassandraDirectory, indexPath, IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        
        assertEquals(cassandraDirectory, Util.getCassandraPath(file));
    }

}
