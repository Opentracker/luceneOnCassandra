package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.lucene.cassandra.ACassandraFile;
import org.apache.lucene.cassandra.CassandraClient;
import org.apache.lucene.cassandra.CassandraFileOutputStream;
import org.apache.lucene.cassandra.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraFileOutputStream {
    
    CassandraClient client = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // start cassandra
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        // stop cassandra
    }

    @Before
    public void setUp() throws Exception {
        client = new CassandraClient("localhost", 9160, true, "lucene0", "index0", 16384);
    }

    @After
    public void tearDown() throws Exception {
        client.truncate("index0");
        client.close();
    }

    @Test
    public void testCassandraFileOutputStreamFileNoAppend() {
        
        try {
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/removeMe.txt");
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file);
            
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            int v = 65;
            byte[] expected = { (byte)v };
            cfos.write(65);
            byte[] actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-0".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
        
    }

    @Test
    public void testCassandraFileOutputStreamFileAppend() {
        
        try {
           
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/removeMe.txt");
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file, true);
                       
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            cfos.write(65);
            cfos.write(65);
            cfos.write(65);
            
            int v = 65;
            byte[] expected = { (byte)v };
            byte[] actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-0".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-1".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-2".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
    }

}