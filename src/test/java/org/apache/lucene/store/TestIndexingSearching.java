package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.cassandra.CassandraClient;
import org.apache.lucene.cassandra.ColumnOrientedDirectory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIndexingSearching {
    
    CassandraClient client = null;
    int blockSize = 16384; 
    static String testFilePath = "test";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (System.getProperty("testFilePath") != null) {
            testFilePath = System.getProperty("testFilePath");
        }        
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        
        try {
            client = new CassandraClient("localhost", 9160, true, "lucene1", "index1", blockSize);
        } catch (IOException e) {
            fail("exception is not expected");
        }
        
        assertNotNull(client);
        client.truncate("index1");
    }

    @After
    public void tearDown() throws Exception {
        client.truncate("index1");
    }

    @Test
    public void testIndexCreateTwice() {
        try {
            /* 
             * index 1 time on directory A
             * and index 1 more time on the same directory A
             * search and the result should be same.
             * 
             * TODO make a better indexing constructor and method. 
             */
            String[] args = {"-cfs", "-docs", testFilePath,  "-index", "index1", "-keyspace", "lucene1", "-column-family", "index1"};
            
            IndexFiles.main(args);        
            IndexFiles.main(args);
            
            
            ColumnOrientedDirectory cod = new ColumnOrientedDirectory(client, blockSize);
            String[] filesname = cod.getFileNames();
            
            List<String> expectedFiles = new ArrayList<String>();
            expectedFiles.add("/_1.cfe");
            expectedFiles.add("/_1.cfs");
            expectedFiles.add("/_1.si");
            expectedFiles.add("/index1");
            expectedFiles.add("/segments_2");
            expectedFiles.add("/segments.gen");
            
            for (String filename : filesname) {
                expectedFiles.remove(filename);
            }
            
            assertEquals(expectedFiles.toString(), 0, expectedFiles.size());

        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
        
    }
    
    @Test
    public void testUpdate() {
        try {
            /* index 1 time 
             * find the term which is not available
             * index again on the directory but include the term.
             * find the term which is now available.
             * TODO make a better indexing constructor and method. 
             */
            String[] args = {"-update", "-docs", testFilePath, "-index", "index1", "-keyspace", "lucene1", "-column-family", "index1"};
            IndexFiles.main(args);
            Search search = new Search("lucene1", "index1", 16384, "index1");
            int total = search.searchOn("apple");
            search.close();
            
            assertEquals(0, total);
            
            
            File file = new File(testFilePath + "/hello.txt");
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWritter = new BufferedWriter(fileWriter);
            bufferWritter.write("apple");
            bufferWritter.close();
            
            IndexFiles.main(args);
            search = new Search("lucene1", "index1", 16384, "index1");
            total = search.searchOn("apple");
            search.close();
            
            assertEquals(1, total);
            
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        } finally {
            try {
                File file = new File(testFilePath + "/hello.txt");
                FileWriter fileWriter = new FileWriter(file, false);
                BufferedWriter bufferWritter = new BufferedWriter(fileWriter);
                bufferWritter.write("hello how are you doing?");
                bufferWritter.close();
            } catch (Exception e) {

            }
        }
    }
    
    @Test
    public void testForceMerge() {
        try {
            /* index 1 time 
             * find the term which is not available
             * index again on the directory but include the term.
             * find the term which is now available.
             * TODO make a better indexing constructor and method. 
             */
            String[] args = {"-update", "-merge", "-docs", testFilePath, "-index", "index1", "-keyspace", "lucene1", "-column-family", "index1"};
            IndexFiles.main(args);

        } catch (Exception e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }
   

}
