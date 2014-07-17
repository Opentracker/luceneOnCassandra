package org.apache.lucene.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.opentracker.test.OpentrackerTestBase;

import org.apache.cassandra.utils.ByteBufferUtil;
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

    static CassandraClient client = null;

    static ColumnOrientedFile cof = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            client =
                    new CassandraClient(cassandraHost, rpcPort, frameMode,
                            keyspace, columnFamily, blockSize);
            cof = new ColumnOrientedFile(client);
        } catch (IOException e) {
            fail("exception is not expected");
        }

        assertNotNull(client);
        assertNotNull(cof);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client.truncate(columnFamily);
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

        FileDescriptor fileDescriptor =
                new FileDescriptor("testsegments.gen", blockSize);
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
        FileDescriptor fileDescriptor =
                new FileDescriptor("testsegments.gen", blockSize);

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

    @Test
    public void testSetFileDescriptor() {
        try {
            FileDescriptor fileDescriptor =
                    new FileDescriptor("removeMe.txt", blockSize);
            cof.setFileDescriptor(fileDescriptor.getName(), fileDescriptor);
            // assert during testGetFileDescriptor
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

    @Test
    public void testGetFileDescriptor() {
        try {
            FileDescriptor fileDescriptor =
                    new FileDescriptor("removeMe.txt", blockSize);
            FileDescriptor actual =
                    cof.getFileDescriptor(fileDescriptor.getName(), blockSize);
            assertEquals("removeMe.txt", actual.getName());
            assertEquals(blockSize, actual.getBlockSize());
            assertEquals(0, actual.getLength());
            assertEquals(false, actual.isDeleted());
            assertEquals(0, actual.getBlocks().size());
        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

    @Test
    public void testDeleteFileBlocks() {
        try {
            // start setting up test values.
            Map<byte[], byte[]> columnValues = new HashMap<byte[], byte[]>();
            columnValues.put("BLOCK-0".getBytes(), "value-0".getBytes());
            columnValues.put("BLOCK-1".getBytes(), "value-1".getBytes());
            columnValues.put("BLOCK-2".getBytes(), "value-2".getBytes());
            columnValues.put("BLOCK-3".getBytes(), "value-3".getBytes());
            columnValues.put("BLOCK-4".getBytes(), "value-4".getBytes());
            columnValues.put("BLOCK-5".getBytes(), "value-5".getBytes());
            columnValues.put("BLOCK-6".getBytes(), "value-6".getBytes());
            client.setColumns(ByteBufferUtil.bytes("removeMe.txt"),
                    columnValues);

            FileDescriptor fileDescriptor =
                    new FileDescriptor("removeMe.txt", blockSize);

            Iterator<Entry<byte[], byte[]>> iter =
                    columnValues.entrySet().iterator();

            int dataOffset = 0;
            int i = 0;
            while (iter.hasNext()) {
                Entry<byte[], byte[]> eBlock = iter.next();
                FileBlock existingBlock = new FileBlock();

                int currentLength = (new String(eBlock.getValue())).length();
                existingBlock.setDataLength(currentLength);
                existingBlock.setBlockSize(blockSize);
                existingBlock.setDataOffset(dataOffset);
                existingBlock.setBlockNumber(i);
                existingBlock.setBlockName("BLOCK-" + i);

                dataOffset += currentLength;
                i++;

                if (fileDescriptor.getBlocks().size() == 0) {
                    fileDescriptor.addFirstBlock(existingBlock);
                } else {
                    if (iter.hasNext()) {
                        Entry<byte[], byte[]> nBlock = iter.next();

                        currentLength =
                                (new String(nBlock.getValue())).length();
                        FileBlock nextBlock = new FileBlock();
                        nextBlock.setDataLength(currentLength);
                        nextBlock.setBlockSize(blockSize);
                        nextBlock.setDataOffset(dataOffset);
                        nextBlock.setBlockNumber(i);
                        nextBlock.setBlockName("BLOCK-" + i);
                        fileDescriptor.addBlock(existingBlock);
                        fileDescriptor.insertBlock(existingBlock, nextBlock,
                                true);
                    } else {
                        // last one, just add
                        fileDescriptor.addLastBlock(existingBlock);
                    }

                    dataOffset += currentLength;
                    i++;
                }
            }

            cof.setFileDescriptor(fileDescriptor.getName(), fileDescriptor);

            columnValues.put("BLOCK-7".getBytes(), "value-4".getBytes());
            columnValues.put("BLOCK-8".getBytes(), "value-5".getBytes());
            columnValues.put("BLOCK-9".getBytes(), "value-6".getBytes());
            client.setColumns(ByteBufferUtil.bytes("removeMe.txt"),
                    columnValues);
            // end setting up test values.

            // delete only the blocks specified in the file descriptor.
            cof.deleteFileBlocks(fileDescriptor, true);
            Map<byte[], byte[]> actuals =
                    client.getColumns("removeMe.txt".getBytes());
            assertEquals(4, actuals.size()); // 3 file blocks + 1 descriptor.
            assertEquals("null", Arrays.toString(actuals.get("Block-6")));
            assertNotNull(Arrays.toString(actuals.get("Block-7")));
            assertNotNull(Arrays.toString(actuals.get("Block-8")));
            assertNotNull(Arrays.toString(actuals.get("Block-9")));

            // delete everything.
            cof.deleteFileBlocks(fileDescriptor, false);
            actuals = client.getColumns("removeMe.txt".getBytes());
            assertEquals(1, actuals.size()); // only the descriptor.

        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }
    }

    @Test
    public void testRenameFile() {

        try {

            List<FileBlock> blocks = new LinkedList<FileBlock>();

            // start preparing data
            BlockMap blocksToFlush = new BlockMap();
            blocksToFlush.put("BLOCK-0", "123");
            FileBlock fileBlock = new FileBlock();
            fileBlock.setBlockName(0);
            fileBlock.setBlockSize(16384);
            fileBlock.setDataOffset(0);
            fileBlock.setDataLength("123".length());
            blocks.add(fileBlock);

            blocksToFlush.put("BLOCK-1", "4567");
            fileBlock = new FileBlock();
            fileBlock.setBlockName(1);
            fileBlock.setBlockSize(16384);
            fileBlock.setDataOffset(3);
            fileBlock.setDataLength("4567".length());
            blocks.add(fileBlock);

            blocksToFlush.put("BLOCK-2", "78910");
            fileBlock = new FileBlock();
            fileBlock.setBlockName(2);
            fileBlock.setBlockSize(16384);
            fileBlock.setDataOffset(7);
            fileBlock.setDataLength("78910".length());
            blocks.add(fileBlock);

            FileDescriptor currentFileDescriptor =
                    new FileDescriptor("/old/oldFile.txt", 16384);
            currentFileDescriptor.setLastAccessed(System.currentTimeMillis());
            currentFileDescriptor.setLastModified(System.currentTimeMillis());
            currentFileDescriptor.setLength(12);
            currentFileDescriptor.setDeleted(false);
            currentFileDescriptor.setBlocks(blocks);

            cof.writeFileBlocks(currentFileDescriptor, blocksToFlush);

            // end preparing data

            FileDescriptor nextFileDescriptor =
                    new FileDescriptor("/new/newFile.txt", 16384);

            boolean result =
                    cof.renameFile(currentFileDescriptor, nextFileDescriptor);
            assertTrue(result);

        } catch (IOException e) {
            e.printStackTrace();
            fail("exception is not expected");
        }

    }

}
