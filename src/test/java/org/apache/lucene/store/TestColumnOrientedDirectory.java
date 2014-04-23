package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.cassandra.CassandraClient;
import org.apache.lucene.cassandra.ColumnOrientedDirectory;
import org.apache.lucene.cassandra.FileDescriptor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnOrientedDirectory {

    CassandraClient client = null;
    ColumnOrientedDirectoryTestable cod = null;
    int blocksize = 16384; 
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        testColumnOrientedDirectory();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testColumnOrientedDirectory() {

        try {
            client = new CassandraClient("localhost", 9160, true, "lucene1", "index1", blocksize);
            cod = new ColumnOrientedDirectoryTestable(client);
        } catch (IOException e) {
            fail("exception is not expected");
            e.printStackTrace();
        }
        
        assertNotNull(client);
        assertNotNull(cod);
    }
    
    @Test
    public void testSetFileDescriptor() {
        FileDescriptor fd = new FileDescriptor("tests.gen", blocksize);
        fd.setDeleted(false);
        try {
            cod.setFileDescriptor(fd);
        } catch (IOException e) {
            fail("exception is not expected");
        }
    }

    @Test
    public void testGetFileNames() {
        try {
            String[] files = cod.getFileNames();
            assertTrue("at least 1 file must be found", files.length > 0);
        } catch (IOException e) {
            fail("exception is not expected");
            e.printStackTrace();
        }
    }

    @Test
    public void testGetFileDescriptorString() {
        try {
            FileDescriptor fd = cod.getFileDescriptor("tests.gen");
            assertNotNull(fd);
            assertEquals(fd.getName(), "tests.gen");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetFileDescriptorStringBoolean() {
        try {
            FileDescriptor fd = cod.getFileDescriptor("notexists", false);
            assertNull(fd);
            
            fd = cod.getFileDescriptor("notExistsThenCreate", true);
            assertNotNull(fd);
            FileDescriptor checkFD = cod.getFileDescriptor("notExistsThenCreate");
            assertNotNull(checkFD);
            assertEquals("notExistsThenCreate", checkFD.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    
    class ColumnOrientedDirectoryTestable extends ColumnOrientedDirectory {

        public ColumnOrientedDirectoryTestable(CassandraClient cassandraClient) {
            super(cassandraClient, blocksize);
        }

        /* Cannot test ColumnOrientedDirectory getFileDescriptor because method
         * modifier is protected. Hence this workaround.
         * 
         * @see org.apache.lucene.cassandra.ColumnOrientedDirectory#getFileDescriptor(java.lang.String)
         */
        @Override
        public FileDescriptor getFileDescriptor(String fileName)
                throws IOException {
            return super.getFileDescriptor(fileName);
        }

        /* Cannot test ColumnOrientedDirectory getFileDescriptor because method
         * modifier is protected. Hence this workaround.
         * 
         * (non-Javadoc)
         * @see org.apache.lucene.cassandra.ColumnOrientedDirectory#getFileDescriptor(java.lang.String, boolean)
         */
        @Override
        public FileDescriptor getFileDescriptor(String fileName,
                boolean createIfNotFound) throws IOException {
            return super.getFileDescriptor(fileName, createIfNotFound);
        }

    }
    
}
