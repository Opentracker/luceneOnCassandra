package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.nio.channels.FileChannel;

import org.apache.lucene.cassandra.ACassandraFile;
import org.apache.lucene.cassandra.CassandraFileOutputStream;
import org.apache.lucene.cassandra.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraFileOutputStream {

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
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCassandraFileOutputStreamFileNoAppend() {
        
        try {
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/removeMe.txt");
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file);
            
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            cfos.write(65);
            // TODO, check content
            
            
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
        
    }

    @Test
    public void testCassandraFileOutputStreamFileAppend() {
        
        try {
            System.out.println("step 1");
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/removeMe.txt");
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file, true);
            
            System.out.println("step 2");
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            System.out.println("step 3");
            cfos.write(65);
            // TODO, check content
            
            System.out.println("step 4");
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
    }
    





}
