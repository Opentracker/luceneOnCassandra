package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.cassandra.FileBlock;
import org.apache.lucene.cassandra.FileDescriptor;
import org.apache.lucene.cassandra.FileDescriptorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileBlock {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testBasic() {
        FileBlock fb = new FileBlock();
        fb.setBlockName(0);
        
        assertEquals("BLOCK-0", fb.getBlockName());
        assertEquals(0, fb.getBlockNumber());
        assertEquals(0, fb.getBlockOffset());
        assertEquals(0, fb.getBlockSize());
        assertEquals(0, fb.getDataLength());
        assertEquals(0, fb.getDataOffset());
        assertEquals(0, fb.getDataPosition());
        assertEquals(0, fb.getLastDataOffset());
        assertEquals(0, fb.getPositionOffset());
    }
    
    @Test
    public void testOther() {
  
        try {
            byte[] b = "{\"lastModified\":1395207481867,\"name\":\"_0.cfs\",\"length\":18806,\"blocks\":[{\"columnName\":\"BLOCK-0\",\"blockSize\":16384,\"dataLength\":5855,\"dataOffset\":0,\"blockNumber\":0},{\"columnName\":\"BLOCK-1\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":5855,\"blockNumber\":1},{\"columnName\":\"BLOCK-2\",\"blockSize\":16384,\"dataLength\":1412,\"dataOffset\":14047,\"blockNumber\":2},{\"columnName\":\"BLOCK-3\",\"blockSize\":16384,\"dataLength\":925,\"dataOffset\":15459,\"blockNumber\":3},{\"columnName\":\"BLOCK-4\",\"blockSize\":16384,\"dataLength\":2422,\"dataOffset\":0,\"blockNumber\":4}],\"deleted\":false,\"lastAccessed\":1395207481867}".getBytes();
            FileDescriptor fd = FileDescriptorUtils.fromBytes(b, 16384);
            
            FileBlock b3 = fd.getBlocks().get(2);
            assertEquals("BLOCK-2", b3.getBlockName());
            
            FileBlock cloneBlock = (FileBlock) b3.clone();
            assertNotSame(b3, cloneBlock);
            assertSame(b3.getClass(), cloneBlock.getClass());
            b3 = null;
            assertNotSame(b3, cloneBlock);
            
            assertEquals("BLOCK-2", cloneBlock.getBlockName());
            assertEquals(1412, cloneBlock.getDataLength());
            assertEquals(14047, cloneBlock.getDataOffset());
            assertEquals(16384, cloneBlock.getBlockSize());
            assertEquals(2, cloneBlock.getBlockNumber());

        } catch (IOException e) {
            fail("exception is not expected.");
        }
    }


}
