package org.apache.lucene.cassandra.trash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>ColumnOrientedDirectory</code> captures the mapping of the concepts
 * of a directory to a column family in Cassandra. Specifically, it treats each
 * row in the column family as a file underneath the directory.
 * 
 * <p>
 * This class in turn relies on the {@link CassandraClient} for all low-level
 * gets and puts to the Cassandra server. More importantly, it does not require
 * that the {@link CassandraClient} to be familiar with the notion of Lucene
 * directories. Rather, it transparently translates those notions to column
 * families. In so doing, it ends up hiding the Cassandra layer from its
 * consumers.
 * </p>
 */
public class ColumnOrientedDirectory {

    private static Logger logger = LoggerFactory
            .getLogger(ColumnOrientedDirectory.class);

    /**
     * @param cassandraDirectory
     */
    public ColumnOrientedDirectory() {
    }

    // The name of the column that holds the file descriptor.
    protected static final String descriptorColumn = "DESCRIPTOR";

    // The list of meta-columns currently defined for each file (or row).
    protected static final List<byte[]> systemColumns = new ArrayList<byte[]>();
    static {
        systemColumns.add(descriptorColumn.getBytes());
    }

    final static ColumnOrientedDirectory SINGLETON =
            new ColumnOrientedDirectory();

    /**
     * @return the names of the files in this directory
     * @throws IOException
     */
    public String[] getFileNames() throws IOException {
        logger.trace("getFileNames");
        byte[][] keys = CassandraClient.CASSANDRA_CLIENT.getKeys(systemColumns);
        List<String> fileNames = new ArrayList<String>();
        for (byte[] key : keys) {
            fileNames.add(new String(key));
        }
        return fileNames.toArray(new String[] {});
    }

    /**
     * Return the file descriptor for the file of the given name. If the file
     * cannot be found, then return null, instead of trying to create it.
     * 
     * @param fileName
     *            the name of the file
     * @return the descriptor for the given file
     * @throws IOException
     */
    public FileDescriptor getFileDescriptor(String fileName) throws IOException {
        logger.trace("getFileDescriptor {}", fileName);
        return getFileDescriptor(fileName, false);
    }

    // The default size of a block, which in turn maps to a cassandra column.
    public static final int DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024;

    /**
     * Return the file descriptor for the file of the given name.
     * 
     * @param fileName
     *            the name of the file
     * @param createIfNotFound
     *            if the file wasn't found, create it
     * @return the descriptor for the given file
     * @throws IOException
     */
    public FileDescriptor getFileDescriptor(String fileName,
            boolean createIfNotFound) throws IOException {
        logger.trace("getFileDescriptor {} with createIfNotFound {}", fileName,
                createIfNotFound);
        FileDescriptor fileDescriptor =
                FileDescriptorUtils.fromBytes(CassandraClient.CASSANDRA_CLIENT
                        .getColumn(fileName.getBytes(),
                                descriptorColumn.getBytes()));
        if (fileDescriptor == null && createIfNotFound) {
            logger.info(
                    "fileDescriptor {} is null, creating a new file descriptor.",
                    fileName);
            fileDescriptor = new FileDescriptor(fileName, DEFAULT_BLOCK_SIZE);
            setFileDescriptor(fileDescriptor);
        }
        return fileDescriptor;
    }

    /**
     * Save the given file descriptor.
     * 
     * @param fileDescriptor
     *            the file descriptor being saved
     * @throws IOException
     */
    public void setFileDescriptor(FileDescriptor fileDescriptor)
            throws IOException {
        logger.trace("setFileDescriptor");
        BlockMap blockMap = new BlockMap();
        blockMap.put(descriptorColumn,
                FileDescriptorUtils.toString(fileDescriptor));
        CassandraClient.CASSANDRA_CLIENT.setColumns(
                ByteBufferUtil.bytes(fileDescriptor.getName()), blockMap);
    }
}