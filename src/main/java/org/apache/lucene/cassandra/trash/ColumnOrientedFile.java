package org.apache.lucene.cassandra.trash;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.CassandraDirectory;

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
    /**
     * 
     */
    private final CassandraDirectory cassandraDirectory;

    /**
     * @param cassandraDirectory
     */
    public ColumnOrientedFile(CassandraDirectory cassandraDirectory) {
        this.cassandraDirectory = cassandraDirectory;
    }

    /**
     * Write the given blocks in the file referenced by the given
     * descriptor.
     * 
     * @param fileDescriptor
     *            the descriptor of the file being written to
     * @param blocksToBeWritten
     *            the map of block names to values
     * @throws IOException
     */
    public void writeFileBlocks(FileDescriptor fileDescriptor,
            BlockMap blocksToBeWritten) throws IOException {
        // System.out.println("The file descriptor saved was " +
        // FileDescriptorUtils.toJSON(fileDescriptor));
        blocksToBeWritten.put(ColumnOrientedDirectory.descriptorColumn,
                FileDescriptorUtils.toString(fileDescriptor));
        CassandraClient.CASSANDRA_CLIENT.setColumns(
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
        Map<byte[], byte[]> columns =
                CassandraClient.CASSANDRA_CLIENT.getColumns(fileDescriptor.getName()
                        .getBytes(), blockNames);
        BlockMap blockMap = new BlockMap();
        blockMap.putAll(columns);
        return blockMap;
    }

}