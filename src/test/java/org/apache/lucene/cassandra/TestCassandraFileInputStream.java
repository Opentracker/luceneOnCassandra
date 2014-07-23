package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.io.Streams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraFileInputStream extends OpentrackerTestBase {
    
    ACassandraFile file;
    CassandraClient client = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        
        client = new CassandraClient("localhost", 9160, true, keyspace, columnFamily, blockSize);
        
        // prepare some data for test.
        file = new ACassandraFile("/", "test/removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        file.write(67, true);
        file.write(68, true);
        file.write(69, true);
        file.close();
        
        // reopen so the the currentblock and its data position at 0 again.
        file = new ACassandraFile("/", "test/removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
    }

    @After
    public void tearDown() throws Exception {
        client.truncate(columnFamily);
        client.close();
    }

    @Test
    public void testCassandraFileInputStream() {
        
        try {
            CassandraFileInputStream cfs = new CassandraFileInputStream(file);            
            
            int value = cfs.read();
            assertEquals(67, value);
            
            FileChannel fc = cfs.getChannel();
            assertNotNull(fc);
            
            cfs.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
        
    }
    
    @Test
    public void testCopyToByteArray() {
        try {
            byte[] data = Streams.copyToByteArray(new CassandraFileInputStream(file));
            byte[] expected = {67, 68, 69};
            assertArrayEquals(expected, data);
        } catch (Exception e) {
            e.printStackTrace();
            fail("exception is not expected.");
        }
      
    }
    
}
