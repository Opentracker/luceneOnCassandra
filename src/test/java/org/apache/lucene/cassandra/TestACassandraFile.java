package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.opentracker.test.OpentrackerTestBase;

public class TestACassandraFile extends OpentrackerTestBase {
    
    CassandraClient client = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        try {
            client = new CassandraClient(cassandraHost, rpcPort, frameMode, keyspace, columnFamily, blockSize);
        } catch (IOException e) {
            fail("exception is not expected");
        }
        
        assertNotNull(client);
    }

    @After
    public void tearDown() throws Exception {
        client.truncate(columnFamily);
    }

    @Test
    public void testRead() {
        
        try {
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            
            // C B A
            byte[] b = {67, 66, 65};
            // in cassandra, stored as 434241
            writeFile.write(b, 0, b.length);
            writeFile.close();
            
            ACassandraFile readFile = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, blockSize);
            int value = readFile.read();
            assertEquals(67, value);
            
            value = readFile.read();
            //assertEquals(66, value);
            
            value = readFile.read();
            assertEquals(65, value);
            
            // TODO fix me
            value = readFile.read();
            assertEquals(-1, value);
            readFile.close();
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
    }
    
    @Test
    public void testRead1() {
        //ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        //writeFile.write(67, true);
        //writeFile.write(66, true);
        //writeFile.write(65, true);
        // in cassandra store as 
        // block-0 43 
        // block-1 42
        // block-2 41
    }

}
