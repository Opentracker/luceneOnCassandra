package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;

import org.apache.lucene.cassandra.FileBlock;
import org.apache.lucene.cassandra.FileDescriptor;
import org.apache.lucene.cassandra.FileDescriptorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileDescriptor {

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
    public void testFileDescriptor() {
        FileDescriptor fd = new FileDescriptor("testrow", 16384);
        
        Date now = new Date();
        assertEquals("testrow", fd.getName());
        assertEquals(0, fd.getLength());
        assertTrue(fd.getLastAccessed() <= now.getTime());
        assertTrue(fd.getLastModified() <= now.getTime());
        assertEquals(16384, fd.getBlockSize());
        assertEquals(0, fd.getBlocks().size());
        assertFalse(fd.isDeleted());
        
   
        fd.setName("anotherName");
        assertEquals("anotherName", fd.getName());
        
        fd.setLength(15333);
        assertEquals(15333, fd.getLength());
        
        Date then = new Date();
        fd.setLastAccessed(then.getTime());
        fd.setLastModified(then.getTime());
        assertTrue(fd.getLastAccessed() >= now.getTime());
        assertTrue(fd.getLastModified() >= now.getTime());
        
        fd.setBlockSize(32768);
        assertEquals(32768, fd.getBlockSize());
        
        LinkedList<FileBlock> blocks = new LinkedList<FileBlock>();
        FileBlock fb0 = new FileBlock();
        FileBlock fb1 = new FileBlock();
        FileBlock fb2 = new FileBlock();
        FileBlock fb3 = new FileBlock();
        FileBlock fb4 = new FileBlock();
        blocks.add(fb0);
        blocks.add(fb1);
        blocks.add(fb2);
        blocks.add(fb3);
        blocks.add(fb4);
        
        fd.setBlocks(blocks);
        
        assertEquals(5, fd.getBlocks().size());
        
        fd.setDeleted(true);
        assertTrue(fd.isDeleted());

    }
    
    @Test
    public void testFileDescriptorBlocks() {
        
        LinkedList<FileBlock> blocks = new LinkedList<FileBlock>();
        FileBlock fb0 = new FileBlock();
        fb0.setBlockName(0);
        FileBlock fb1 = new FileBlock();
        fb1.setBlockName(1);
        FileBlock fb2 = new FileBlock();
        fb2.setBlockName(2);
        FileBlock fb3 = new FileBlock();
        fb3.setBlockName(3);
        FileBlock fb4 = new FileBlock();
        fb4.setBlockName(4);
        blocks.add(fb0);
        blocks.add(fb1);
        blocks.add(fb2);
        blocks.add(fb3);
        blocks.add(fb4);
        
        // start the test
        
        FileDescriptor fd = new FileDescriptor("testrow", 16384);
        fd.setBlocks(blocks);
        
        // sequence => 0 1 2 3 4
        assertEquals("BLOCK-0", fd.getFirstBlock().getBlockName());
        assertEquals("BLOCK-4", fd.getLastBlock().getBlockName());
        assertEquals(5, fd.getBlocks().size());
        
        assertTrue(fd.isFirstBlock(fb0));
        assertFalse(fd.isFirstBlock(fb1));
        assertFalse(fd.isFirstBlock(fb4));
        
        assertFalse(fd.isLastBlock(fb0));
        assertTrue(fd.isLastBlock(fb4));
        
        assertEquals("BLOCK-3", fd.getNextBlock(fb2).getBlockName());
        
        // test add last block
        FileBlock fb5 = new FileBlock();
        fb5.setBlockName(5);
        fd.addLastBlock(fb5);
        
        assertFalse(fd.isLastBlock(fb0));
        assertFalse(fd.isLastBlock(fb4));
        assertTrue(fd.isLastBlock(fb5));
        assertEquals(6, fd.getBlocks().size());
        
        // test add first block
        FileBlock fb6 = new FileBlock();
        fb6.setBlockName(6);
        fd.addFirstBlock(fb6);
        
        assertFalse(fd.isFirstBlock(fb0));
        assertTrue(fd.isFirstBlock(fb6));
        assertEquals(7, fd.getBlocks().size());
        
        //  current block sequence 6 0 1 2 3 4 5
        FileBlock fb7 = new FileBlock();
        fb7.setBlockName(7);
        fd.insertBlock(fb4, fb7, true);
        
        //  current block sequence 6 0 1 2 3 4 7 5
        assertEquals("BLOCK-3", fd.getBlocks().get(4).getBlockName());
        assertEquals("BLOCK-4", fd.getBlocks().get(5).getBlockName());
        assertEquals("BLOCK-7", fd.getBlocks().get(6).getBlockName());
        assertEquals("BLOCK-5", fd.getBlocks().get(7).getBlockName());
        assertEquals(8, fd.getBlocks().size());

        FileBlock fb8 = new FileBlock();
        fb8.setBlockName(8);
        fd.insertBlock(fb7, fb8, false);
        
        //  current block sequence 6 0 1 2 3 4 8 7 5
        assertEquals("BLOCK-3", fd.getBlocks().get(4).getBlockName());
        assertEquals("BLOCK-4", fd.getBlocks().get(5).getBlockName());
        assertEquals("BLOCK-8", fd.getBlocks().get(6).getBlockName());
        assertEquals("BLOCK-7", fd.getBlocks().get(7).getBlockName());
        assertEquals(9, fd.getBlocks().size());
        
        // test replace
        FileBlock fb9 = new FileBlock();
        fb9.setBlockName(9);
        fd.replaceBlock(fb8, fb9);
        
        //  current block sequence 6 0 1 2 3 4 9 7 5
        assertEquals("BLOCK-3", fd.getBlocks().get(4).getBlockName());
        assertEquals("BLOCK-4", fd.getBlocks().get(5).getBlockName());
        assertEquals("BLOCK-9", fd.getBlocks().get(6).getBlockName());
        assertEquals("BLOCK-7", fd.getBlocks().get(7).getBlockName());
        assertEquals(9, fd.getBlocks().size());
        
        // test remove.
        fd.removeBlock(fb9);
        
        //  current block sequence 6 0 1 2 3 4 7 5
        assertEquals("BLOCK-3", fd.getBlocks().get(4).getBlockName());
        assertEquals("BLOCK-4", fd.getBlocks().get(5).getBlockName());
        assertEquals("BLOCK-7", fd.getBlocks().get(6).getBlockName());
        assertEquals("BLOCK-5", fd.getBlocks().get(7).getBlockName());
        assertEquals(8, fd.getBlocks().size());
        
        // test remove.
        fd.removeBlock(fb6);

        //  current block sequence 0 1 2 3 4 7 5
        assertEquals("BLOCK-0", fd.getBlocks().get(0).getBlockName());
        assertEquals("BLOCK-3", fd.getBlocks().get(3).getBlockName());
        assertEquals("BLOCK-5", fd.getBlocks().get(6).getBlockName());
        assertEquals("BLOCK-0", fd.getFirstBlock().getBlockName());
        assertEquals("BLOCK-5", fd.getLastBlock().getBlockName());
        assertEquals(7, fd.getBlocks().size());
        
        // test create block
        assertEquals(8, fd.getNextBlockNumber());
        
        // test createBlock
        FileBlock newBlock = fd.createBlock();
        assertEquals("BLOCK-9", newBlock.getBlockName());
        assertEquals(16384, newBlock.getBlockSize());
        assertEquals(0, newBlock.getDataLength());
        
        //  current block sequence 0 1 2 3 4 7 5
        assertEquals(7, fd.getBlocks().size());
        
        fd.addLastBlock(newBlock);
        assertEquals(8, fd.getBlocks().size());
        
    }
    
    @Test
    public void testBlocks() {
        byte[] b = "{\"lastModified\":1395383941846,\"name\":\"_0.cfs\",\"length\":1869293,\"blocks\":[{\"columnName\":\"BLOCK-0\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":0},{\"columnName\":\"BLOCK-1\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":1},{\"columnName\":\"BLOCK-2\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":2},{\"columnName\":\"BLOCK-3\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":3},{\"columnName\":\"BLOCK-4\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":4},{\"columnName\":\"BLOCK-5\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":5},{\"columnName\":\"BLOCK-6\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":6},{\"columnName\":\"BLOCK-7\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":7},{\"columnName\":\"BLOCK-8\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":8},{\"columnName\":\"BLOCK-9\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":9},{\"columnName\":\"BLOCK-10\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":10},{\"columnName\":\"BLOCK-11\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":11},{\"columnName\":\"BLOCK-12\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":12},{\"columnName\":\"BLOCK-13\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":13},{\"columnName\":\"BLOCK-14\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":14},{\"columnName\":\"BLOCK-15\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":15},{\"columnName\":\"BLOCK-16\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":16},{\"columnName\":\"BLOCK-17\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":17},{\"columnName\":\"BLOCK-18\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":18},{\"columnName\":\"BLOCK-19\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":19},{\"columnName\":\"BLOCK-20\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":20},{\"columnName\":\"BLOCK-21\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":21},{\"columnName\":\"BLOCK-22\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":22},{\"columnName\":\"BLOCK-23\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":23},{\"columnName\":\"BLOCK-24\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":24},{\"columnName\":\"BLOCK-25\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":25},{\"columnName\":\"BLOCK-26\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":26},{\"columnName\":\"BLOCK-27\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":27},{\"columnName\":\"BLOCK-28\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":28},{\"columnName\":\"BLOCK-29\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":29},{\"columnName\":\"BLOCK-30\",\"blockSize\":16384,\"dataLength\":340,\"dataOffset\":0,\"blockNumber\":30},{\"columnName\":\"BLOCK-31\",\"blockSize\":16384,\"dataLength\":8192,\"dataOffset\":340,\"blockNumber\":31},{\"columnName\":\"BLOCK-32\",\"blockSize\":16384,\"dataLength\":7852,\"dataOffset\":8532,\"blockNumber\":32}],\"deleted\":false,\"lastAccessed\":1395383941846}".getBytes();
        
        int i = 0;
        
        try {
            FileDescriptor fd = FileDescriptorUtils.fromBytes(b, 16384);
            for (FileBlock fb : fd.getBlocks()) {
                assertEquals(i, fb.getBlockNumber());
                i++;
            }
            
        } catch (IOException e) {
            fail("exception is not expected.");
        }
    }

    @Test
    public void testSync() {
        // TODO
    }

    @Test
    public void testValid() {
        // TODO
    }

}
