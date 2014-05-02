package org.apache.lucene.store;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.lucene.cassandra.CassandraClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import junit.framework.TestCase;

public class TestWriteRead extends TestCase {

    CassandraDirectory cassandraDirectory;
    static CassandraClient client = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            client = new CassandraClient("localhost", 9160, true, "lucene1", "index1", 16384);
        } catch (IOException e) {
            fail("exception is not expected");
        }
        
        assertNotNull(client);
        client.truncate("index1");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client.truncate("index1");
    }

    @Before
    public void setUp() throws IOException {
        cassandraDirectory =
                new CassandraDirectory("lucene1", "index1", 100, 100);
    }

    @After
    public void tearDown() throws IOException {
        if (cassandraDirectory != null) {
            cassandraDirectory.close();
        }
    }

    public void testWriteFile() throws IOException {
        IndexOutput indexOutput =
                cassandraDirectory.createOutput("hello.txt", IOContext.DEFAULT);
        indexOutput.writeString("hello sun");
        indexOutput.writeString("hello mercury");
        indexOutput.flush();

        indexOutput =
                cassandraDirectory.createOutput("hihi.txt", IOContext.DEFAULT);
        indexOutput.writeString("hihi world");
        indexOutput.flush();
        
        indexOutput = 
                cassandraDirectory.createOutput("segments_1", IOContext.DEFAULT);
        indexOutput.writeString("hihi world");
        indexOutput.flush();
    }

    public void testReadFile() throws IOException {
        IndexInput indexInput =
                cassandraDirectory.openInput("hello.txt", IOContext.DEFAULT);
        String fileContent = indexInput.readString();
        assertEquals("hello sun", fileContent);

        indexInput =
                cassandraDirectory.openInput("hihi.txt", IOContext.DEFAULT);
        fileContent = indexInput.readString();
        assertEquals("hihi world", fileContent);
    }

    public void testUpdate() {

    }

    public void testSearch() {

    }
    
    public void testReadStrings() throws IOException {
        IndexInput indexInput =
                cassandraDirectory.openInput("segments_1", IOContext.DEFAULT);
        String actualDataPoint = indexInput.readString();
        assertEquals("hihi world", actualDataPoint);
    }

}
