package org.apache.lucene.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>ColumnOrientedFile</code> captures the mapping of the concept
 * of a file to a row in Cassandra. Specifically, it considers each column
 * in the row as a block in the file. Furthermore, it uses one of the
 * columns to hold the {@link FileDescriptor} for the file, in the form of a
 * JSON string (which serves to make the "file" readable by other,
 * potentially disparate, clients).
 * 
 * <p>
 * This class in turn relies on the {@link CassandraClient} for all
 * low-level gets and puts to the Cassandra server. More importantly, it
 * does not require that the {@link CassandraClient} be familiar with the
 * notion of Lucene files. Rather, it transparently translates those notions
 * to rows within the column family denoting the directory. In so doing, it
 * ends up hiding the Cassandra layer from its consumers.
 * </p>
 */
public class ColumnOrientedFile {
    
    private static Logger logger = LoggerFactory.getLogger(ColumnOrientedFile.class);
    
    // The name of the column that holds the file descriptor.
    protected static final String descriptorColumn = "DESCRIPTOR";
    private CassandraClient cassandraClient = null;
    
    public ColumnOrientedFile(CassandraClient cassandraClient) {
        this.cassandraClient = cassandraClient;
    }
    
    /**
     * Write the given blocks in the file referenced by the given
     * descriptor.
     * 
     * Write the blocks into column family reference by key rowname fileDescriptor.
     * 
     * @param fileDescriptor
     *            the descriptor of the file being written to
     * @param blocksToBeWritten
     *            the map of block names to values
     * @throws IOException
     */
    public void writeFileBlocks(FileDescriptor fileDescriptor,
            BlockMap blocksToBeWritten) throws IOException {
        logger.trace("writeFileBlocks {}", fileDescriptor.getName());
        // System.out.println("The file descriptor saved was " +
        // FileDescriptorUtils.toJSON(fileDescriptor));
        blocksToBeWritten.put(descriptorColumn,
                FileDescriptorUtils.toString(fileDescriptor));
        cassandraClient.setColumns(
                ByteBufferUtil.bytes(fileDescriptor.getName()),
                blocksToBeWritten);
    }

    /**
     * Read the given blocks from the file referenced by the given
     * descriptor.
     * 
     * @param fileDescriptor
     *            the descriptor of the file being read
     * @param blockNames
     *            the (unique) set of block names to read from
     * @return the map of block names to values
     * @throws IOException
     */
    public BlockMap readFileBlocks(FileDescriptor fileDescriptor,
            Set<byte[]> blockNames) throws IOException {
        logger.trace("readFileBlocks {}", fileDescriptor.getName());
        Map<byte[], byte[]> columns = cassandraClient.getColumns(fileDescriptor.getName().getBytes(), blockNames);
        BlockMap blockMap = new BlockMap();
        blockMap.putAll(columns);
        return blockMap;
    }
    
    public FileDescriptor getFileDescriptor(String fileName, int blockSize) throws IOException {
        byte[] fd = cassandraClient.getColumn(fileName.getBytes(), descriptorColumn.getBytes());
        return FileDescriptorUtils.fromBytes(fd, blockSize);
    }
    
    public void setFileDescriptor(String fileName, FileDescriptor fileDescriptor) throws IOException {
        ByteBuffer key = ByteBufferUtil.bytes(fileName);
        Map<byte[], byte[]> column = new HashMap<byte[], byte[]>();
        column.put(descriptorColumn.getBytes(), FileDescriptorUtils.toBytes(fileDescriptor));
        cassandraClient.setColumns(key, column);
    }

    /**
     * Delete file block.
     *
     * @param fileDescriptor
     *            the file descriptor specified which file to remove.
     *
     * @param onlyDeleteFileBlocksWithinFileDescriptor
     *            ' if true, then only delete file blocks specified by this file
     *            descriptor. if false, it will remove all the file blocks
     *            except file descriptor.
     *
     * @throws IOException
     */
    public void deleteFileBlocks(FileDescriptor fileDescriptor,
            boolean onlyDeleteFileBlocksWithinFileDescriptor)
            throws IOException {

        if (onlyDeleteFileBlocksWithinFileDescriptor) {

            List<FileBlock> fileBlocks = fileDescriptor.getBlocks();

            for (FileBlock fileBlock : fileBlocks) {
                Map<byte[], byte[]> column = new HashMap<>();
                column.put(fileBlock.getBlockName().getBytes(), null);
                cassandraClient.setColumns(
                        ByteBufferUtil.bytes(fileDescriptor.getName()), column);
            }

        } else {

            Map<byte[], byte[]> fileBlocks =
                    cassandraClient.getColumns(fileDescriptor.getName()
                            .getBytes());

            for (Entry<byte[], byte[]> fileBlock : fileBlocks.entrySet()) {
                String fileName = new String(fileBlock.getKey());
                if (descriptorColumn.equals(fileName)) {
                    continue;
                }
                Map<byte[], byte[]> column = new HashMap<>();
                column.put(fileBlock.getKey(), null);
                cassandraClient.setColumns(
                        ByteBufferUtil.bytes(fileDescriptor.getName()), column);
            }

        }
    }

    /**
     * rename a file by copy as many information as possible from
     * <code>currentFileDescriptor</code> except filename to
     * <code>nextFileDescriptor</code>.
     * 
     * @param currentFileDescriptor
     *            the current file descriptor which is about to rename.
     * 
     * @param nextFileDescriptor
     *            the next file descriptor which is about to copy to.
     * 
     * @return true if the rename process is success.
     * 
     * @throws IOException
     *             any exception during this rename process.
     */
    public boolean renameFile(FileDescriptor currentFileDescriptor,
            FileDescriptor nextFileDescriptor) throws IOException {

        try {

            // get all data
            List<FileBlock> blocks = currentFileDescriptor.getBlocks();
            long blockSize = currentFileDescriptor.getBlockSize();
            long lastAccessed = currentFileDescriptor.getLastAccessed();
            long lastModified = currentFileDescriptor.getLastModified();
            long length = currentFileDescriptor.getLength();
            boolean isDeleted = currentFileDescriptor.isDeleted();

            Set<byte[]> columnNames = new HashSet<byte[]>();
            for (FileBlock block : blocks) {
                columnNames.add(block.getBlockName().getBytes());
            }

            BlockMap currentFileBlocks =
                    readFileBlocks(currentFileDescriptor, columnNames);

            // copy to another new rowkey
            nextFileDescriptor.setBlockSize(blockSize);
            nextFileDescriptor.setLastAccessed(lastAccessed);
            nextFileDescriptor.setLastModified(lastModified);
            nextFileDescriptor.setLastModified(length);
            nextFileDescriptor.setDeleted(isDeleted);
            nextFileDescriptor.setBlocks(blocks);

            writeFileBlocks(nextFileDescriptor, currentFileBlocks);

            // delete the previous row.
            currentFileDescriptor.setDeleted(true);
            cassandraClient
                    .setColumns(ByteBufferUtil.bytes(currentFileDescriptor
                            .getName()), null);

            return true;

        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

}
