package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.nio.channels.FileChannel;
import java.util.Arrays;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.lucene.cassandra.ACassandraFile;
import org.apache.lucene.cassandra.CassandraClient;
import org.apache.lucene.cassandra.CassandraFileOutputStream;
import org.apache.lucene.cassandra.File;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCassandraFileOutputStream extends OpentrackerTestBase {
    
    CassandraClient client = null;

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
        client = new CassandraClient("localhost", 9160, true, keyspace, columnFamily, blockSize);
    }

    @After
    public void tearDown() throws Exception {
        client.truncate(columnFamily);
        client.close();
    }

    @Test
    public void testCassandraFileOutputStreamFileNoAppend() {
        
        try {
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file);
            
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            int v = 65;
            byte[] expected = { (byte)v };
            cfos.write(65);
            byte[] actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-0".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
        
    }

    @Test
    public void testCassandraFileOutputStreamFileAppend() {
        
        try {
           
            File file = new ACassandraFile("/test/cassandraFileOutputStreamFile/", "removeMe.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            CassandraFileOutputStream cfos = new CassandraFileOutputStream(file, true);
                       
            FileChannel fc = cfos.getChannel();
            assertNotNull(fc);
            
            cfos.write(65);
            cfos.write(65);
            cfos.write(65);
            
            int v = 65;
            byte[] expected = { (byte)v };
            byte[] actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-0".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-1".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            actualValue = client.getColumn("/test/cassandraFileOutputStreamFile/removeMe.txt".getBytes(), "BLOCK-2".getBytes());
            assertEquals(Arrays.toString(expected), Arrays.toString(actualValue));
            
            cfos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
    }
    
    /* TODO, how should the data be store in cassandra?
    /* currently, output of builder.String() is
     * => {
     *      "version" : 132
     *    }
     *  
     *  but in cassandra, it is 7d ( which is } )
     */
    @Test
    public void testElasticSearch() {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, new BytesStreamOutput());
            builder.prettyPrint();
            builder.startObject();
            builder.field("version", 132);
            builder.endObject();
            builder.flush();
            System.out.println("=> " + builder.string());
            File stateFile = new ACassandraFile("/", "test/removeMe1.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            CassandraFileOutputStream fos = new CassandraFileOutputStream(stateFile);
            BytesReference bytes = builder.bytes();
            bytes.writeTo(fos);
            fos.getChannel().force(true);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
    }
    
}