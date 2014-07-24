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
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, 4);
            
            // C B A
            byte[] b = {70, 69, 68, 67, 66, 65};
            // in cassandra, stored as 46454443
            //                                        4241
            writeFile.write(b, 0, b.length);
            writeFile.close();
            
            ACassandraFile readFile = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, 4);
            int value = readFile.read();
            assertEquals(70, value);
            
            value = readFile.read();
            assertEquals(69, value);
            
            value = readFile.read();
            assertEquals(68, value);
            
            value = readFile.read();
            assertEquals(67, value);
            
            value = readFile.read();
            assertEquals(66, value);
            
            value = readFile.read();
            assertEquals(65, value);
            
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
        try {
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            writeFile.write(67, true);
            writeFile.write(66, true);
            writeFile.write(65, true);
            writeFile.close();
            // in cassandra store as 
            // block-0 43 
            // block-1 42
            // block-2 41
            
            ACassandraFile readFile = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, 4);
            int value = readFile.read();
            assertEquals(67, value);
            
            value = readFile.read();
            assertEquals(66, value);
            
            value = readFile.read();
            assertEquals(65, value);
            
            value = readFile.read();
            assertEquals(-1, value);
            readFile.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
    }

    @Test
    public void  testListFiles() {

        try {
            // prepare some data
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            writeFile.write(67, true);
            writeFile.write(66, true);
            writeFile.write(65, true);
            writeFile.close();

            // test.
            ACassandraFile file = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, blockSize);
            File[] files = file.listFiles();
            file.close();

            assertTrue(files.length == 1);
            assertEquals("/removeMe.txt", files[0].getName());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}