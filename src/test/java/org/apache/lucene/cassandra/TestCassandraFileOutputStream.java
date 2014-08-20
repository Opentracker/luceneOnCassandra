package org.apache.lucene.cassandra;

import static org.junit.Assert.*;

import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
    
    @Test
    public void testElasticSearchNoAppend() {
        try {
            // prepare data.
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, new BytesStreamOutput());
            builder.prettyPrint();
            builder.startObject();
            builder.field("version", 132);
            builder.endObject();
            builder.flush();
            File stateFile = new ACassandraFile("/", "test/removeMe1.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            CassandraFileOutputStream fos = new CassandraFileOutputStream(stateFile);
            BytesReference bytes = builder.bytes();
            fos.write(bytes.array(), 0, 10);
            bytes.writeTo(fos);
            fos.getChannel().force(true);
            fos.close();

            // test
            StringBuffer actual = new StringBuffer();
            byte[] filename = "/test/removeMe1.txt".getBytes();
            Map<byte[], byte[]> blocks = client.getColumns(filename);
            for (Entry<byte[], byte[]> block : blocks.entrySet()) {
                String column = new String(block.getKey());
                if (column.equals("DESCRIPTOR")) {
                    continue;
                }
                actual.append(Util.bytesToHex(block.getValue()));
            }

            assertEquals(actual.toString(), "7B0A20202276657273696F6E22203A203133320A7D");
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }

    }

    @Test
    public void testElasticSearchAppend() {
        try {
            // prepare data.
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, new BytesStreamOutput());
            builder.prettyPrint();
            builder.startObject();
            builder.field("version", 12345);
            builder.endObject();
            builder.flush();
            File stateFile = new ACassandraFile("/", "test/removeMe1.txt", IOContext.DEFAULT, true, keyspace, columnFamily, blockSize);
            byte[] b = "ABC".getBytes();
            stateFile.write(b, 0, b.length);
            CassandraFileOutputStream fos = new CassandraFileOutputStream(stateFile, true);
            BytesReference bytes = builder.bytes();
            fos.write(bytes.array(), 0, 10);
            bytes.writeTo(fos);
            fos.getChannel().force(true);
            fos.close();

            // test
            StringBuffer actual = new StringBuffer();
            Map<String, String> actuals = new LinkedHashMap<String, String>();
            byte[] filename = "/test/removeMe1.txt".getBytes();
            Set<byte []> columns = new LinkedHashSet<byte[]>();
            columns.add("BLOCK-0".getBytes());
            columns.add("BLOCK-1".getBytes());
            columns.add("BLOCK-2".getBytes());
            Map<byte[], byte[]> blocks = client.getColumns(filename, columns);
            for (Entry<byte[], byte[]> block : blocks.entrySet()) {
                actuals.put(new String(block.getKey()), Util.bytesToHex(block.getValue()));
            }

            actual.append(actuals.get("BLOCK-0"));
            actual.append(actuals.get("BLOCK-1"));
            actual.append(actuals.get("BLOCK-2"));

            assertEquals(actual.toString(), "4142437B0A20202276657273697B0A20202276657273696F6E22203A2031323334350A7D");
        } catch (Exception e) {
            e.printStackTrace();
            fail("fail is not expected");
        }
        
    }
    
}