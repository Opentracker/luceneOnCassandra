package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.lucene.store.IOContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraDirectory extends  OpentrackerTestBase {
    
    CassandraDirectory cassandraDirectory;
    static CassandraClient client = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            client = new CassandraClient("localhost", rpcPort, frameMode, keyspace, columnFamily, blockSize);
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
        try {
            CassandraFile file = new CassandraFile("/test/", "testFile", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            cassandraDirectory = CassandraDirectory.open(file, IOContext.DEFAULT, null, keyspace, columnFamily, blockSize, bufferSize);
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail not expected");
        }
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testCassandraDirectory() {
        assertNotNull(cassandraDirectory);
    }

    @Test
    public void testClose() {
        
    }

    @Test
    public void testOpen() {
        assertTrue(cassandraDirectory instanceof SimpleCassandraDirectory);
        assertNotNull(cassandraDirectory);
    }

    @Test
    public void testSetLockFactory() {
        // when open to the directory, this method is called.
    }

    @Test
    public void testListAll() {
        try {
            cassandraDirectory.listAll();
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail not expected");
        }
    }

    @Test
    public void testFileExists() {
        boolean notExist = cassandraDirectory.fileExists("NotExists");
        assertFalse(notExist);
    }

    @Test
    public void testFileModified() {
        /* fileModified exist in lucene 4.6.0 but removed in lucene 4.8.0
        CassandraFile file = new CassandraFile("/test/", "testFile", IOContext.READ, true, keyspace, columnFamily, blockSize);
        long fileModified = cassandraDirectory.fileModified(file, "testFile");
        assertTrue(fileModified > 0);
        */
    }

    @Test
    public void testFileLength() {
        // TODO still need to implement this.
    }
    
    @Test
    public void testCreateOutput() {
        try {
            cassandraDirectory.createOutput("deleteMe", IOContext.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail not expected");
        }
    }

    @Test
    public void testDeleteFile() {
        try {
            cassandraDirectory.deleteFile("deleteMe");
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail not expected");
        }
    }

    @Test
    public void testEnsureCanWrite() {
        try {
            cassandraDirectory.ensureCanWrite("testFilePermission");
        } catch (IOException e) {
            e.printStackTrace();
            fail("fail not expected");
        }
    }

    @Test
    public void testOnIndexOutputClosed() {
        // not important to test for now.
    }

    @Test
    public void testSyncCollectionOfString() {
        // not important to test for now.
    }

    @Test
    public void testGetLockID() {
        assertEquals("lucene-56c6130e", cassandraDirectory.getLockID());
    }

    @Test
    public void testGetDirectory() {
        CassandraFile directory = cassandraDirectory.getDirectory();
        assertNotNull(directory);        
    }

    @Test
    public void testToString() {
        // not important to test for now.
    }

    @Test
    public void testSetReadChunkSize() {
        // not important to test for as it is deprecated since lucene 4.5
    }

    @Test
    public void testGetReadChunkSize() {
        // not important to test for as it is deprecated since lucene 4.5
    }

    @Test
    public void testFsync() {
        // not important to test for now.
    }

}
