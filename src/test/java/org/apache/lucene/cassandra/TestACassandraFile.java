package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        // ---test public File[] listFiles() ---
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
            // in cassandra, it is /removeMe.txt;
            assertEquals("/removeMe.txt", files[0].getName());

        } catch (Exception e) {
            e.printStackTrace();
        }
        // ---test public File[] listFiles() ---

        // ---test public File[] listFiles(CassandraFileFilter filter) ---
        try {
            // prepare some data
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            writeFile.write(67, true);
            writeFile.write(66, true);
            writeFile.write(65, true);
            writeFile.close();

            // test.
            ACassandraFile file = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, blockSize);
            File[] files = file.listFiles(null);
            file.close();

            assertTrue(files.length == 1);
            // in cassandra, it is /removeMe.txt;
            assertEquals("/removeMe.txt", files[0].getName());

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            // prepare some data
            ACassandraFile writeFile = new ACassandraFile("/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            writeFile.write(67, true);
            writeFile.write(66, true);
            writeFile.write(65, true);
            writeFile.close();

            // test.
            ACassandraFile file = new ACassandraFile("/", "removeMe.txt", IOContext.READ, true, keyspace, columnFamily, blockSize);
            CassandraFileFilter filter = new CassandraFileFilter() {

                @Override
                public boolean accept(File pathname) {
                    return false;
                }
            };
            File[] files = file.listFiles(filter);
            file.close();

            // in cassandra, it is /removeMe.txt; but because the accept always false, return
            // nothing instead.
            assertTrue(files.length == 0);

        } catch (Exception e) {
            e.printStackTrace();
        }
        // ---test public File[] listFiles(CassandraFileFilter filter) ---

    }

    /**
     * Note: This default to always lucene0 and index0, which is not good. Putting in
     * here and to improve and change in the future.
     */
    @Test
    public void testACassandraFile() {
        
        CassandraClient lucene0 = null;
        
        // == Test ACassandraFile(File parent, String child) ==
        try {
            lucene0 = new CassandraClient(cassandraHost, rpcPort, frameMode, "lucene0", "index0", blockSize);
            
            // prepare data
            String child = "123";
            ACassandraFile parent = new ACassandraFile("/test/removeMe.txt");
           
            ACassandraFile file = new ACassandraFile(parent, child);
            file.close();
            
            List<byte[]> column = new ArrayList<byte[]>();
            column.add("DESCRIPTOR".getBytes());
            
            byte[][] arrays = lucene0.getKeys(column, 1024);
            
            assertEquals(2, arrays.length);
            assertEquals("/test/removeMe.txt", new String(arrays[0]));
            assertEquals("/test/removeMe.txt/123", new String(arrays[1]));
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        } finally {
            if (lucene0 != null) {
                try {
                    lucene0.truncate("index0");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // == Test ACassandraFile(File parent, String child) ==
        

        // == Test ACassandraFile(String canonicalPath)  ==
        try {
            lucene0 = new CassandraClient(cassandraHost, rpcPort, frameMode, "lucene0", "index0", blockSize);
            
            // prepare data
            ACassandraFile testFile = new ACassandraFile("/test/dummy/removeMe.txt");
            testFile.close();
            assertEquals("/test/dummy", testFile.getParent(true));
            assertEquals("/test/dummy/removeMe.txt", testFile.getName());
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        } finally {
            if (lucene0 != null) {
                try {
                    lucene0.truncate("index0");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // == Test ACassandraFile(String canonicalPath)  ==
        
        
        // == ACassandraFile(String directory, String name, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize)  ==

        // prepare data
        ACassandraFile testFile = new ACassandraFile("/test/dummy/", "removeMeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
        testFile.close();
        assertEquals("/test/dummy", testFile.getParent(true));
        assertEquals("/test/dummy/removeMeMe.txt", testFile.getName());

        // == ACassandraFile(String directory, String name, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize)  ==

    }
}