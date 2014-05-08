package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.lucene.store.Directory.IndexInputSlicer;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSimpleCassandraDirectory extends OpentrackerTestBase {
    
    private SimpleCassandraDirectory scd = null;
    static CassandraClient client = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            client = new CassandraClient(cassandraHost, rpcPort, frameMode, keyspace, columnFamily, blockSize);
        } catch (IOException e) {
            fail("exception is not expected");
        }
        
        assertNotNull(client);
        client.truncate(columnFamily);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client.truncate(columnFamily);
    }

    @Before
    public void setUp() throws Exception {
        CassandraFile file = new CassandraFile("/test/", "testFile", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        byte[] content = "hello world".getBytes();
        file.write(content, 0, content.length);
        try {
            scd = new SimpleCassandraDirectory(file, IOContext.DEFAULT, null, keyspace, columnFamily, blockSize, bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSimpleCassandraDirectory() {
        assertNotNull(scd);
    }

    @Test
    public void testOpenInput() {
        try {
            IndexInput input = scd.openInput("testFile", IOContext.READ);
            assertNotNull(input);
            byte[] content = new byte[11];; 
            input.readBytes(content, 0, 11);
            assertEquals("hello world", new String(content));
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

    @Test
    public void testCreateSlicer() {
        try {
            IndexInputSlicer slicer = scd.createSlicer("testFile", IOContext.READ);
            IndexInput input = slicer.openSlice("test open slice", 0, 5);
            assertNotNull(input);
            byte[] content = new byte[5];; 
            input.readBytes(content, 0, 5);
            //assertTrue("hello".equals(new String(content)));
            assertEquals("hello", new String(content));
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

}
