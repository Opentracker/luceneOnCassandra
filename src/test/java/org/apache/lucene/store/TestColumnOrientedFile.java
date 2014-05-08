package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.lucene.cassandra.BlockMap;
import org.apache.lucene.cassandra.CassandraClient;
import org.apache.lucene.cassandra.ColumnOrientedFile;
import org.apache.lucene.cassandra.FileBlock;
import org.apache.lucene.cassandra.FileDescriptor;
import org.apache.lucene.cassandra.Util;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnOrientedFile extends OpentrackerTestBase {
    
    CassandraClient client = null;
    ColumnOrientedFile cof = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        testColumnOrientedFile();
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testColumnOrientedFile() {

        try {
            client = new CassandraClient(cassandraHost, rpcPort, frameMode, keyspace, columnFamily, blockSize);
            cof = new ColumnOrientedFile(client);
        } catch (IOException e) {
            fail("exception is not expected");
        }
        
        assertNotNull(client);
        assertNotNull(cof);
    }

    @Test
    public void testWriteFileBlocks() {
        
        // write
        BlockMap blocksToFlush = new BlockMap();
        blocksToFlush.put("BLOCK-0", "123");
        
        FileBlock existingBlock = new FileBlock();
        existingBlock.setDataLength("123".length());
        existingBlock.setBlockSize(blockSize);
        existingBlock.setDataOffset(0);
        existingBlock.setBlockNumber(0);
        existingBlock.setBlockName("BLOCK-0");
        
        FileBlock preFragment = (FileBlock) existingBlock.clone();
        
        FileDescriptor fileDescriptor = new FileDescriptor("testsegments.gen", blockSize);
        fileDescriptor.setLastAccessed(System.currentTimeMillis());
        fileDescriptor.setLastModified(System.currentTimeMillis());
        fileDescriptor.setLength("123".length());
        // not really distinctive the insertBlock below.
        fileDescriptor.insertBlock(existingBlock, preFragment, true);
        
        try {
            cof.writeFileBlocks(fileDescriptor, blocksToFlush);
            // assert during testReadFileBlocks.
            
        } catch (IOException e) {
            fail("exception is not expected");
        }
    }
    


    @Test
    public void testReadFileBlocks() {

        // read 
        Set<byte[]> blockNames = new HashSet<byte[]>();
        byte[] column0 = "BLOCK-0".getBytes();
        blockNames.add(column0);
        FileDescriptor fileDescriptor = new FileDescriptor("testsegments.gen", blockSize);
        
        BlockMap bm;
        try {
            bm = cof.readFileBlocks(fileDescriptor, blockNames);
            
            for (Map.Entry<byte[], byte[]> columnEntry : bm.entrySet()) {
                String columnName = new String(columnEntry.getKey());
                byte[] columnValue = columnEntry.getValue();
                assertEquals("313233", Util.bytesToHex(columnValue));
                assertEquals("BLOCK-0", columnName);
            }
        } catch (IOException e) {
            fail("exception is not expected");
        }

    }

}
