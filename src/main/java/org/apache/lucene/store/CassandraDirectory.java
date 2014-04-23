package org.apache.lucene.store;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory.IndexInputSlicer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>CassandraDirectory</code> maps the concept of a Lucene directory to
 * a column family that belongs to a certain keyspace located in a given
 * Cassandra server. Furthermore, it stores each file under this directory as a
 * row in that column family.
 * 
 * <p>
 * In particular, files are broken down into blocks (whose sizes are capped),
 * where each block (see {@link FileBlock}) is stored as the value of a column
 * in the corresponding row. As per
 * http://wiki.apache.org/cassandra/CassandraLimitations, this is the
 * recommended approach for dealing with large objects, which Lucene files tend
 * to be. Furthermore, a descriptor of the file (see {@link FileDescriptor})
 * that outlines a map of blocks therein is stored as one of the columns in that
 * row as well. Think of this descriptor as an inode for Cassandra-based files.
 * </p>
 * 
 * <p>
 * The exhaustive mapping of a Lucene directory (file) to a Cassandra column
 * family (row) is captured in the {@link ColumnOrientedDirectory} (
 * {@link ColumnOrientedFile}) inner-class. Specifically, they interpret
 * Cassandra's data model in terms of that of Lucene. More importantly, these
 * are the only two inner-classes that have a foot in both the Lucene and
 * Cassandra camps.
 * </p>
 * 
 * <p>
 * All writes to a file in this directory occur through a
 * {@link CassandraIndexOutput}, which puts the data flushed from a write-behind
 * buffer into the fitting set of blocks. By the same token, all reads from a
 * file in this directory occur through a {@link CassandraIndexInput}, which
 * gets the data needed by a read-ahead buffer from the right set of blocks.
 * </p>
 * 
 * <p>
 * The last (but not the least) inner-class, {@link CassandraClient}, acts as a
 * facade for a Thrift-based Cassandra client. In short, it provides operations
 * to get/put rows/columns in the column family and keyspace associated with
 * this directory.
 * </p>
 * 
 * <p>
 * Unlike Lucandra, which attempts to bridge the gap between Lucene and
 * Cassandra at the document-level, the {@link CassandraDirectory} is
 * self-sufficient in the sense that it does not require a re-write of other
 * components in the Lucene stack. In other words, one may use the
 * {@link CassandraDirectory} in conjunction with the {@link IndexWriter} and
 * {@link IndexReader}, as you would any other kind of Lucene {@link Directory}.
 * Moreover, given the the data unit that is transferred to and from Cassandra
 * is a large-sized block, one may expect fewer round trips, and hence better
 * throughput, from the {@link CassandraDirectory}.
 * <p>
 * 
 * <p>
 * In conclusion, this directory attempts to marry the rich search-based query
 * language of Lucene with the distributed fault-tolerant database that is
 * Cassandra. By delegating the responsibilities of replication, durability and
 * elasticity to the directory, we free the layers above from such
 * non-functional concerns. Our hope is that users will choose to make their
 * large-scale indices instantly scalable by seamlessly migrating them to this
 * type of directory (using {@link Directory#copyTo(Directory)}).
 * </p>
 * 
 * @author Karthick Sankarachary
 */
public class CassandraDirectory extends Directory {

    private static Logger logger = LoggerFactory
            .getLogger(CassandraDirectory.class);

    // The default size of a block, which in turn maps to a cassandra column.
    public static final int DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024;

    // The default size of the buffer, which is managed by the index output.
    public static final int DEFAULT_BUFFER_SIZE = 1 * DEFAULT_BLOCK_SIZE;

    // The default host where the cassandra server is located.
    public static final String DEFAULT_CASSANDRA_HOST = "localhost";

    // The default port where the cassandra server is listening.
    public static final int DEFAULT_CASSANDRA_PORT = 9160;

    // The default flag indicating whether the cassandra server is framed.
    public static final boolean DEFAULT_CASSANDRA_FRAMED = true;

    // The default keyspace in which to store cassandra directories.
    public static final String DEFAULT_CASSANDRA_KEYSPACE = "lucene1";

    // The default column family
    public static final String DEFAULT_CASSANDRA_COLUMN_FAMILY = "index1";

    // The name of every column that holds a file block starts with this prefix.
    protected static final String BLOCK_COLUMN_NAME_PREFIX = "BLOCK-";

    // The name of the column that holds the file descriptor.
    protected static final String descriptorColumn = "DESCRIPTOR";

    // The list of meta-columns currently defined for each file (or row).
    protected static final List<byte[]> systemColumns = new ArrayList<byte[]>();
    static {
        systemColumns.add(descriptorColumn.getBytes());
    }

    // The reference to the cassandra client that talks to the thrift server.
    protected CassandraClient cassandraClient;

    // The keyspace in which to read/write cassandra directories and files.
    protected String keyspace;

    // The name of the column family that maps to this cassandra directory.
    protected String columnFamily;

    // The current size of the block to write out to a column.
    protected int blockSize;

    // The current size of the buffer that (ideally) is big enough to hold one
    // or
    // more file blocks. In essence, the write (read) buffer acts as a
    // write-behind (read-ahead) cache that performs a lazy write (read) only
    // when
    // the data is evicted (or flushed) from the cache. Given that, we should
    // try
    // to write and read data in block multiples.
    protected int bufferSize;

    protected ColumnOrientedDirectory columnOrientedDirectory;

    /**
     * Construct a Cassandra-based directory that maps to the given column
     * family, which is located in the default keyspace.
     * 
     * @param columnFamily
     *            the name of the column family that denotes this directory
     * @throws IOException
     */
    public CassandraDirectory(String columnFamily) throws IOException {
        this(DEFAULT_CASSANDRA_KEYSPACE, columnFamily);
    }

    /**
     * Construct a Cassandra-based directory that maps to the given column
     * family, which is located in the given keyspace. Note that it uses the
     * default block and buffer sizes.
     * 
     * @param keyspace
     *            the name of the keyspace in which to find the column family
     * @param columnFamily
     *            the name of the column family that denotes this directory
     * @throws IOException
     */
    public CassandraDirectory(String keyspace, String columnFamily)
            throws IOException {
        this(keyspace, columnFamily, DEFAULT_BLOCK_SIZE, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Construct a Cassandra-based directory that maps to the given column
     * family, which is located in the given keyspace. Moreover, it uses the
     * given block size as the column size, and the given buffer size for the
     * write-behind and read-ahead cache.
     * 
     * @param keyspace
     *            the name of the keyspace in which to find the column family
     * @param columnFamily
     *            the name of the column family that denotes this directory
     * @param blockSize
     *            the size of the file block
     * @param bufferSize
     *            the size of the read/write buffer
     * @throws IOException
     */
    public CassandraDirectory(String keyspace, String columnFamily,
            int blockSize, int bufferSize) throws IOException {
        this(DEFAULT_CASSANDRA_HOST, DEFAULT_CASSANDRA_PORT,
                DEFAULT_CASSANDRA_FRAMED, keyspace, columnFamily, blockSize,
                bufferSize);
    }

    /**
     * Construct a Cassandra-based directory that maps to the given column
     * family, which is located in the given keyspace. In particular, this
     * directory talks to the cassandra server running on the given host and
     * port.
     * 
     * @param host
     *            the host where the cassandra server is located
     * @param port
     *            the port where the cassandra server is listening
     * @param keyspace
     *            the name of the keyspace in which to find the column family
     * @param columnFamily
     *            the name of the column family that denotes this directory
     * @throws IOException
     */
    public CassandraDirectory(String host, Integer port, String keyspace,
            String columnFamily) throws IOException {
        this(host, port, DEFAULT_CASSANDRA_FRAMED, keyspace, columnFamily,
                DEFAULT_BLOCK_SIZE, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Construct a Cassandra-based directory that maps to the given column
     * family, which is located in the given keyspace. In particular, this
     * directory talks to the cassandra server running on the given host and
     * port.
     * 
     * @param host
     *            the host where the cassandra server is located
     * @param port
     *            the port where the cassandra server is listening
     * @param framed
     *            a flag to ensure a fully read message every time by preceding
     *            messages with a 4-byte frame size
     * @param keyspace
     *            the name of the keyspace in which to find the column family
     * @param columnFamily
     *            the name of the column family that denotes this directory
     * @param blockSize
     *            the size of the file block
     * @param bufferSize
     *            the size of the read/write buffer
     * @throws IOException
     */
    public CassandraDirectory(String host, Integer port, boolean framed,
            String keyspace, String columnFamily, int blockSize, int bufferSize)
            throws IOException {
        logger.trace(String.format("initializing CassandraDirectory host = '%s' " +
        		"port = '%s' framed = '%s' keyspace = '%s' columnFamily = '%s' " +
        		"blockSize = '%s' bufferSize = '%s'", host, port, framed, keyspace, columnFamily, blockSize, bufferSize));
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.blockSize = blockSize;
        this.bufferSize = bufferSize;
        this.cassandraClient = new CassandraClient(host, port, framed);
        this.columnOrientedDirectory = new ColumnOrientedDirectory();
        this.lockFactory = new SimpleFSLockFactory("lock");
    }
    
    @Override
    public IndexInputSlicer createSlicer(String name, IOContext context)
            throws IOException {
        logger.trace("createSlicer called");
        return super.createSlicer(name, context);
    }

    /**
     * @return the name of the keyspace in which to find the column family
     */
    public String getKeyspace() {
        logger.trace("keyspace " + keyspace);
        return keyspace;
    }

    /**
     * @return the name of the column family that represents this directory
     */
    public String getColumnFamily() {
        logger.trace("columnFamily " + columnFamily);
        return columnFamily;
    }

    /**
     * @return the size of the file block
     */
    public long getBlockSize() {
        logger.trace("getBlockSize " + blockSize);
        return blockSize;
    }

    /**
     * @return the size of the read/write buffer
     */
    public long getBufferSize() {
        logger.trace("getBufferSize " + bufferSize);
        return bufferSize;
    }

    /**
     * Creates a new, empty file in the directory with the given file name.
     * 
     * @return a stream that writes into this file
     */
    @Override
    public IndexOutput createOutput(String fileName, IOContext context)
            throws IOException {
        logger.trace("createOutput " + fileName);
        ensureOpen();
        isOpen = true;
        return new CassandraIndexOutput(fileName, bufferSize);
    }

    /**
     * Open an existing file in the directory with the given file name.
     * 
     * @return a stream that reads from an existing file.
     */
    @Override
    public IndexInput openInput(String fileName, IOContext context)
            throws IOException {
        ensureOpen();
        logger.trace("openInput " + fileName);
        return new CassandraIndexInput(fileName, bufferSize);
    }

    /**
     * Returns an array of strings, one for each (non-deleted) file in the
     * directory.
     * 
     * @throws NoSuchDirectoryException
     *             if the directory is not prepared for any write operations
     *             (such as {@link #createOutput(String, IOContext)}).
     * @throws IOException
     *             in case of other IO errors
     */
    @Override
    public String[] listAll() throws IOException {
        logger.trace("listAll ");
        ensureOpen();
        return columnOrientedDirectory.getFileNames();
    }

    /**
     * @return true if a file with the given name exists
     */
    @Override
    public boolean fileExists(String fileName) throws IOException {
        ensureOpen();
        logger.trace("fileExists " + fileName);
        try {
            return fileLength(fileName) >= 0;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Returns the length of a file in the directory. This method follows the
     * following contract:
     * <ul>
     * <li>Throws {@link FileNotFoundException} if the file does not exist
     * <li>Returns a value &ge;0 if the file exists, which specifies its length.
     * </ul>
     * 
     * @param name
     *            the name of the file for which to return the length.
     * @throws FileNotFoundException
     *             if the file does not exist.
     * @throws IOException
     *             if there was an IO error while retrieving the file's length.
     */
    @Override
    public long fileLength(String fileName) throws IOException {
        ensureOpen();
        logger.trace("fileLength " + fileName);
        FileDescriptor descriptor =
                columnOrientedDirectory.getFileDescriptor(fileName);
        if (descriptor == null) {
            throw new IOException("Could not find descriptor for file "
                    + fileName);
        }
        return descriptor.getLength();
    }

    /**
     * lucene 4.6.0 does not have fileModified abstract file anymore.
     * 
     * @return the time the named file was last modified
     */
    // @Override
    public long fileModified(String fileName) throws IOException {
        ensureOpen();
        logger.trace("fileModified " + fileName);
        FileDescriptor descriptor =
                columnOrientedDirectory.getFileDescriptor(fileName);
        if (descriptor == null) {
            throw new IOException("Could not find descriptor for file "
                    + fileName);
        }
        return descriptor.getLastModified();
    }

    /**
     * lucene 4.6.0 does not have touchFile abstract file anymore.
     * 
     * Set the modified time of an existing file to now.
     */
    // @Override
    public void touchFile(String fileName) throws IOException {
        ensureOpen();
        logger.trace("touchFile " + fileName);
        try {
            FileDescriptor fileDescriptor =
                    columnOrientedDirectory.getFileDescriptor(fileName);
            fileDescriptor.setLastModified(System.currentTimeMillis());
            columnOrientedDirectory.setFileDescriptor(fileDescriptor);
        } catch (Exception e) {
            throw new IOException("Could not touch file " + fileName, e);
        }
    }

    /**
     * Removes an existing file in the directory.
     */
    @Override
    public void deleteFile(String fileName) throws IOException {
        ensureOpen();
        logger.trace("deleteFile " + fileName);
        FileDescriptor fileDescriptor =
                columnOrientedDirectory.getFileDescriptor(fileName);
        if (fileDescriptor != null) {
            fileDescriptor.setDeleted(true);
            columnOrientedDirectory.setFileDescriptor(fileDescriptor);
        }
    }

    volatile protected boolean isOpen = true;

    @Override
    public void close() throws IOException {
        logger.trace("close ");
        isOpen = false;
    }

    protected LockFactory lockFactory;

    @Override
    public void sync(Collection<String> names) throws IOException {
        logger.trace("sync ");
        ensureOpen();
        for (String name : names) {
            logger.debug("sync name {}", name);
            //IndexOutput io = this.createOutput(name, IOContext.DEFAULT);
            //io.flush();
        }
    }

    @Override
    public Lock makeLock(String name) {
        logger.trace("makeLock " + name);
        return lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        logger.trace("clearLock " + name);
        if (lockFactory != null) {
            lockFactory.clearLock(name);
        }
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        logger.trace("setLockFactory ");
        assert lockFactory != null;
        this.lockFactory = lockFactory;
        this.lockFactory.setLockPrefix(this.getLockID());
    }

    @Override
    public LockFactory getLockFactory() {
        logger.trace("getLockFactory ");
        return this.lockFactory;
    }

    @Override
    protected final void ensureOpen() throws AlreadyClosedException {
        logger.trace("ensureOpen ");
        if (!isOpen)
            throw new AlreadyClosedException("this Directory is closed");
    }

    /**
     * The <code>FileDescriptor</code> captures the meta-data of the file, a la
     * Unix inodes (index nodes). In addition to the usual tidbits such as name,
     * length and timestamps, it carries a flag (@see {@link #deleted}) that
     * indicates whether the file has been deleted or not.
     * 
     * <p>
     * The data in the file is indexed through a ordered list of
     * {@link FileBlock} s, where each block denotes a contiguous portion of the
     * file data. By walking through the blocks sequentially in the given order,
     * one can retrieve the entire contents of the file. A psuedo-random access
     * of the file can be effected by loading into memory the entire block to
     * which the (random) file pointer maps, and then positioning the file
     * pointer within the in-memory block.
     * </p>
     */
    public static class FileDescriptor {
        // The name of the file.
        private String name;

        // The length of the file.
        private long length;

        // A flag indicating whether the file has been deleted. Currently, we
        // cannot
        // delete rows from a column family, which means that it is not
        // physically
        // possible to delete a file. To workaround this issue, which is
        // described
        // in detail at https://issues.apache.org/jira/browse/CASSANDRA-293, we
        // rely
        // on this flag instead. Note that, as a side-effect of this design, we
        // could potentially allow cassandra files to be "unremoved", if you
        // will.
        private boolean deleted;

        // The timestamp at which the file was last modified.
        private long lastModified;

        // The timestamp at which the file was last accessed.
        private long lastAccessed;

        // The maximum size of the block that may be stored in a column.
        private long blockSize;

        // The ordered list of blocks in this file.
        private LinkedList<FileBlock> blocks;

        // The number to use for the next block that will be allocated. If it is
        // uninitialized (i.e., -1), then it forces the descriptor to reset it
        // to
        // the highest number of any block in {@link #blocks}.
        private int nextBlockNumber = -1;

        /**
         * Construct a file descriptor for the given file name, using the
         * default block size.
         * 
         * @param fileName
         *            the name of the file
         */
        public FileDescriptor(String fileName) {
            this(fileName, DEFAULT_BLOCK_SIZE);
        }

        /**
         * Construct a file descriptor for the given file name and block size.
         * 
         * @param fileName
         *            the name of the file
         * @param blockSize
         *            the size of the block
         */
        public FileDescriptor(String fileName, long blockSize) {
            setName(fileName);
            setLength(0);
            Date now = new Date();
            setLastAccessed(now.getTime());
            setLastModified(now.getTime());
            setBlockSize(blockSize);
            setBlocks(new LinkedList<FileBlock>());
            String debug = String.format("fileName = '%s' blockSize = '%s'", fileName, blockSize);
            logger.debug(debug);
        }

        /**
         * @return the name of the file
         */
        public String getName() {
            logger.trace("get name {}", name);
            return name;
        }

        /**
         * Rename the file.
         * 
         * @param name
         *            the name of the file
         */
        public void setName(String name) {
            logger.trace("setName {}", name);
            this.name = name;
        }

        /**
         * @return the current length of the file
         */
        public long getLength() {
            logger.trace("getLength {}", length);
            return length;
        }

        /**
         * Resize the file.
         * 
         * @param length
         *            the new length of the file
         */
        public void setLength(long length) {
            logger.trace("setLength {}", length);
            this.length = length;
        }

        /**
         * @return true if the file has been deleted
         */
        public boolean isDeleted() {
            return deleted;
        }

        /**
         * Mark the file as deleted (or undeleted) based on whether the given
         * flag is true (or not).
         * 
         * @param deleted
         *            should the file be marked as deleted?
         */
        public void setDeleted(boolean deleted) {
            logger.trace("setDeleted {}", deleted);
            this.deleted = deleted;
        }

        /**
         * @return the timestamp at which the file was last modified
         */
        public long getLastModified() {
            logger.trace("getLastModified {}", lastModified);
            return lastModified;
        }

        /**
         * Set the timestamp at which the file was last modified.
         * 
         * @param lastModified
         *            the last modified timestamp
         */
        public void setLastModified(long lastModified) {
            logger.trace("setLastModified {}", lastModified);
            this.lastModified = lastModified;
        }

        /**
         * @return the timestamp at which the file was last accessed
         */
        public long getLastAccessed() {
            logger.trace("getLastAccessed {}", lastAccessed);
            return lastAccessed;
        }

        /**
         * Set the timestamp at which the file was last accessed.
         * 
         * @param lastModified
         *            the last accessed timestamp
         */
        public void setLastAccessed(long lastAccessed) {
            logger.trace("setLastAccessed {}", lastAccessed);
            this.lastAccessed = lastAccessed;
        }

        /**
         * @return the maximum size of the block that may be stored in a column
         */
        public long getBlockSize() {
            logger.trace("getBlockSize {}", blockSize);
            return blockSize;
        }

        /**
         * Set the maximum size of the block that may be stored in a column.
         * 
         * @param blockSize
         *            the block size
         */
        public void setBlockSize(long blockSize) {
            logger.trace("setBlockSize {}", blockSize);
            this.blockSize = blockSize;
        }

        /**
         * @return the ordered list of file blocks
         */
        public List<FileBlock> getBlocks() {
            logger.trace("getBlocks {}", blocks.size());
            return blocks;
        }

        /**
         * Set the ordered list of file blocks.
         * 
         * @param blocks
         *            the ordered list of file blocks
         */
        public void setBlocks(List<FileBlock> blocks) {
            if (LinkedList.class.isAssignableFrom(blocks.getClass())) {
                this.blocks = (LinkedList<FileBlock>) blocks;
            } else {
                this.blocks = new LinkedList<FileBlock>(blocks);
            }
        }

        /**
         * @return the first block in the file
         */
        public FileBlock getFirstBlock() {
            logger.trace("getFirstBlock");
            if (blocks.isEmpty()) {
                blocks.add(createBlock());
            }
            return blocks.getFirst();
        }

        /**
         * @return the last block in the file
         */
        public FileBlock getLastBlock() {
            logger.trace("getLastBlock");
            if (blocks.isEmpty()) {
                blocks.add(createBlock());
            }
            return blocks.getLast();
        }

        /**
         * @return true if the given block is the first one in the file
         */
        public boolean isFirstBlock(FileBlock nextBlock) {
            logger.trace("isFirstBlock");
            return getFirstBlock().equals(nextBlock);
        }

        /**
         * @return true if the given block is the last one in the file
         */
        public boolean isLastBlock(FileBlock nextBlock) {
            logger.trace("isLastBlock");
            return getLastBlock().equals(nextBlock);
        }

        /**
         * Return the block that logically follows the given block.
         * 
         * @param block
         *            an existing file block
         * @return the block that logically follows the given block
         */
        public FileBlock getNextBlock(FileBlock block) {
            int blockIndex = blocks.indexOf(block);
            logger.trace("get next block {}", blockIndex);
            return (blockIndex != -1 && blockIndex < (blocks.size() - 1))
                    ? blocks.get(blockIndex + 1) : null;
        }

        /**
         * Add the given block as the last block in the file.
         * 
         * @param newBlock
         *            the block to be appended to the file
         */
        public void addLastBlock(FileBlock newBlock) {
            logger.trace("addLastBlock");
            blocks.addLast(newBlock);
        }

        /**
         * Add the given block as the first block in the file.
         * 
         * @param newBlock
         *            the block to be prepended to the file
         */
        public void addFirstBlock(FileBlock newBlock) {
            logger.trace("addFirstBlock");
            blocks.addFirst(newBlock);
        }

        /**
         * Insert a new block either after or before an existing block, based on
         * whether or not the insertAfter flag is true.
         * 
         * @param existingBlock
         *            an existing file block
         * @param newBlock
         *            a new file block
         * @param insertAfter
         *            whether or not to insert new block after the existing
         *            block
         */
        public void insertBlock(FileBlock existingBlock, FileBlock newBlock,
                boolean insertAfter) {
            logger.trace("insertBlock");
            int existingIndex = blocks.indexOf(existingBlock);
            if (existingIndex == -1) {
                blocks.add(newBlock);
            } else {
                if (insertAfter) {
                    blocks.add(existingIndex + 1, newBlock);
                } else {
                    blocks.add(existingIndex, newBlock);
                }
            }
        }

        /**
         * Replace an existing block with a new block.
         * 
         * @param existingBlock
         *            an existing file block
         * @param newBlock
         *            a new file block
         */
        public void replaceBlock(FileBlock existingBlock, FileBlock newBlock) {
            logger.trace("replaceBlock");
            int existingIndex = blocks.indexOf(existingBlock);
            if (existingIndex != -1) {
                blocks.remove(existingIndex);
                blocks.add(existingIndex, newBlock);
            }
        }

        /**
         * Remove an existing block.
         * 
         * @param existingBlock
         *            an existing block
         * @return the index of the block just removed
         */
        public int removeBlock(FileBlock existingBlock) {
            logger.trace("removeBlock");
            int existingIndex = blocks.indexOf(existingBlock);
            if (existingIndex != -1) {
                blocks.remove(existingBlock);
            }
            return existingIndex;
        }

        /**
         * Create a file block with no data in it. The block number assigned to
         * new blocks is set to auto-increment.
         * 
         * @return the newly created file block
         */
        public FileBlock createBlock() {
            logger.trace("createBlock");
            FileBlock newBlock = new FileBlock();
            newBlock.setBlockName(getNextBlockNumber());
            newBlock.setBlockSize(getBlockSize());
            newBlock.setDataLength(0);
            return newBlock;
        }

        /**
         * Return the block number to use for the next block that is allocated.
         * This number starts from 0 and will auto-increment with each block
         * allocation.
         * 
         * @return the next block number to use
         */
        public int getNextBlockNumber() {
            logger.trace("getNextBlockNumber");
            if (nextBlockNumber == -1) {
                for (FileBlock fileBlock : blocks) {
                    nextBlockNumber =
                            Math.max(nextBlockNumber,
                                    fileBlock.getBlockNumber());
                }
            }
            return ++nextBlockNumber;
        }

    }

    /**
     * A <code>FileBlock</code> denotes a partition of the file whose size is
     * capped by a certain limit (@see {@link #blockSize}). In particular, it
     * refers to a contiguous sequence of bytes, which is stored as the value of
     * a column in the column family that represents this directory.
     * 
     * <p>
     * Ideally, each block in the file (except for maybe the last one) should
     * contain as many bytes as {@link #blockSize}. However, at times, blocks
     * tend to become fragmented in the sense that the {@link blockSize} bytes
     * that was supposed to go in one (ideal) block gets spread across multiple
     * blocks, each containing a portion of the ideal block. This may happen
     * when writes occur after the file pointer is randomly moved across the
     * file. While there are ways to avoid or at least mitigate the
     * fragmentation issue, we currently deal with it as a part of life. In the
     * event a block represents a fragment, it is important to know the offset
     * of the fragment relative to the block where the (fragmented) block
     * begins. This information is captured in {@link FileBlock#dataOffset}.
     * </p>
     * 
     * <p>
     * There is some file block information that is not actually saved in
     * cassandra, but rather calculated on the fly. For example, the
     * {@link FileBlock#blockOffset} is used to keep track of the offset of the
     * block relative to the file in. Similarly, the
     * {@link FileBlock#dataPosition} notes where the file pointer is positioned
     * relative to the block. Both of these fields are calculated after the file
     * descriptor is loaded from Cassandra.
     * </p>
     */
    public static class FileBlock implements Cloneable {
        // The name of the file block.
        private String blockName;

        // The number of the file block.
        private int blockNumber;

        // The maximum size of the file block.
        private long blockSize;

        // The offset of the first byte in this block relative to the file.
        private long blockOffset;

        // The offset within the file block range where the first data byte
        // appears.
        private long dataOffset;

        // The length of the data starting from {@link #dataOffset}
        private int dataLength;

        // The position of the file pointer relative to this block, assuming
        // that
        // pointer is currently inside this block to begin with.
        private int dataPosition;

        /**
         * Construct an empty file block.
         */
        public FileBlock() {
            logger.trace("initialize FileBlock");
        }

        /**
         * @return the name of the file block
         */
        public String getBlockName() {
            logger.trace("getBlockName {}", blockName);
            return blockName;
        }

        /**
         * Set the name of the file block.
         * 
         * @param blockName
         *            the name of the file block
         */
        public void setBlockName(String blockName) {
            logger.trace("setBlockName {}", blockName);
            this.blockName = blockName;
        }

        /**
         * Set the name of the file block based on the given block number.
         * 
         * @param blockNumber
         *            the block number
         */
        public void setBlockName(int blockNumber) {
            logger.debug("blockNumber " + blockNumber);
            this.blockNumber = blockNumber;
            this.blockName = createBlockName(blockNumber);
        }

        /**
         * @return the number of this file block
         */
        public int getBlockNumber() {
            logger.trace("getBlockNumber {}", blockNumber);
            return blockNumber;
        }

        /**
         * Set the number of this file block.
         * 
         * @param blockNumber
         *            the number for this file block
         */
        public void setBlockNumber(int blockNumber) {
            logger.trace("setBlockNumber");
            this.blockNumber = blockNumber;
        }

        /**
         * @return the maximum size of this file block
         */
        public long getBlockSize() {
            logger.trace("getBlockSize");
            return blockSize;
        }

        /**
         * Set the maximum size of this file block.
         * 
         * @param blockSize
         *            the maximum block size
         */
        public void setBlockSize(long blockSize) {
            logger.trace("setBlockSize");
            this.blockSize = blockSize;
        }

        /**
         * @return the offset of the first byte in this block relative to the
         *         file
         */
        public long getBlockOffset() {
            logger.trace("getBlockOffset");
            return blockOffset;
        }

        /**
         * Set the offset of the first byte in this block relative to the file.
         * 
         * @param blockOffset
         *            the new offset for this file block
         */
        public void setBlockOffset(long blockOffset) {
            logger.trace("setBlockOffset {}", blockOffset);
            this.blockOffset = blockOffset;
        }

        /**
         * @return the offset within the file block range where the first data
         *         byte appears
         */
        public long getDataOffset() {
            logger.trace("getDataOffset ");
            return dataOffset;
        }

        /**
         * Set the offset within the file block range where the first data byte
         * appears.
         * 
         * @param dataOffset
         *            the data offset
         */
        public void setDataOffset(long dataOffset) {
            logger.trace("setDataOffset ");
            this.dataOffset = dataOffset;
        }

        /**
         * @return the length of the data starting from {@link #dataOffset}
         */
        public int getDataLength() {
            logger.trace("getDataLength {}", dataLength);
            return dataLength;
        }

        /**
         * Set the length of the data starting from {@link #dataOffset}.
         * 
         * @param dataLength
         *            the new data length
         */
        public void setDataLength(int dataLength) {
            logger.trace("setDataLength ");
            this.dataLength = dataLength;
        }

        /**
         * Calculate the offset of the last byte of data in this file block
         * relative to the block.
         * 
         * @return the last byte's offset relative to the block
         */
        public long getLastDataOffset() {
            logger.trace("getLastDataOffset ");
            return getDataOffset() + getDataLength();
        }

        /**
         * @return the position of the file pointer relative to this block,
         *         assuming that pointer is currently inside this block to begin
         *         with
         */
        public int getDataPosition() {
            logger.trace("getDataPosition {}", dataPosition);
            return dataPosition;
        }

        /**
         * Set the position of the file pointer relative to this block, assuming
         * that pointer is currently inside this block to begin with
         * 
         * @param dataPosition
         *            the new data position
         */
        public void setDataPosition(int dataPosition) {
            logger.trace("setDataPosition {}", dataPosition);
            this.dataPosition = dataPosition;
        }

        public long getPositionOffset() {
            logger.trace("getPositionOffset ");
            return getDataOffset() + getDataPosition();
        }

        /**
         * Create a readable name of the block, derived from it's block number.
         * 
         * @param blockNumber
         *            the number of the block to use in the name
         * @return
         */
        public static String createBlockName(int blockNumber) {
            logger.trace("createBlockName ");
            return BLOCK_COLUMN_NAME_PREFIX + blockNumber;
        }

        /**
         * @return a shallow clone of this file block
         */
        @Override
        public Object clone() {
            logger.trace("clone ");
            try {
                return super.clone();
            } catch (CloneNotSupportedException e) {
                return null;
            }
        }
    }

    // A comparator that checks if two byte arrays are the same or not.
    protected static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR =
            new Comparator<byte[]>() {
                public int compare(byte[] o1, byte[] o2) {
                    if (o1 == null || o2 == null) {
                        return (o1 != null ? -1 : o2 != null ? 1 : 0);
                    }
                    if (o1.length != o2.length) {
                        return o1.length - o2.length;
                    }
                    for (int i = 0; i < o1.length; i++) {
                        if (o1[i] != o2[i]) {
                            return o1[i] - o2[i];
                        }
                    }
                    return 0;
                }
            };

    /**
     * The <code>BlockMap</code> keeps track of file blocks by their names.
     * Given that the name is a byte array, we rely on a custom comparator that
     * knows how to compare such names.
     */
    protected class BlockMap extends TreeMap<byte[], byte[]> {
        private static final long serialVersionUID = 1550200273310875675L;

        /**
         * Define a block map which is essentially a map of a block name (in the
         * form of bytes) to the block data (again, in the form of bytes). Given
         * that byte arrays don't lend themselves to comparison naturally, we
         * pass it a custom comparator.
         */
        public BlockMap() {
            super(BYTE_ARRAY_COMPARATOR);
        }

        /**
         * Put a <key, value> tuple in the block map, where the key is a
         * {@link java.lang.String}.
         * 
         * @param key
         *            a stringified key
         * @param value
         *            a byte array value
         * @return the previously associated value
         */
        public byte[] put(String key, byte[] value) {
            return super.put(key.getBytes(), value);
        }

        /**
         * Put a <key, value> tuple in the block map, where the value is a
         * {@link java.lang.String}.
         * 
         * @param key
         *            a byte array key
         * @param value
         *            a stringified value
         * @return the previously associated value
         */
        public byte[] put(byte[] key, String value) {
            return super.put(key, value.getBytes());
        }

        /**
         * Put a <key, value> tuple in the block map, where both the key and
         * value are a {@link java.lang.String}.
         * 
         * @param key
         *            a stringified key
         * @param value
         *            a stringified value
         * @return the previously associated value
         */
        public byte[] put(String key, String value) {
            return super.put(key.getBytes(), value.getBytes());
        }

        /**
         * Get the value for the given key, which is a {@link java.lang.String}.
         * 
         * @param key
         *            a stringified key
         * @return the currently associated value
         */
        public byte[] get(String key) {
            return super.get(key.getBytes());
        }
    }

    /**
     * A utility for serializing (and deserialize) the file descriptor to (and
     * from) JSON objects.
     */
    public static class FileDescriptorUtils {
        /**
         * Convert the given file descriptor to bytes.
         * 
         * @param fileDescriptor
         * @return
         * @throws IOException
         */
        public static byte[] toBytes(FileDescriptor fileDescriptor)
                throws IOException {
            return toString(fileDescriptor).getBytes();
        }

        /**
         * Convert the given file descriptor to a String.
         * 
         * @param fileDescriptor
         * @return
         * @throws IOException
         */
        public static String toString(FileDescriptor fileDescriptor)
                throws IOException {
            return toJSON(fileDescriptor).toString();
        }

        /**
         * Convert the given file descriptor to a {@link JSONObject}
         * 
         * @param fileDescriptor
         * @return
         * @throws IOException
         */
        public static JSONObject toJSON(FileDescriptor fileDescriptor)
                throws IOException {
            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("name", fileDescriptor.getName());
                jsonObject.put("length", fileDescriptor.getLength());
                jsonObject.put("deleted", fileDescriptor.isDeleted());
                jsonObject
                        .put("lastModified", fileDescriptor.getLastModified());
                jsonObject
                        .put("lastAccessed", fileDescriptor.getLastAccessed());
                JSONArray jsonArray = new JSONArray();
                for (FileBlock fileBlock : fileDescriptor.getBlocks()) {
                    JSONObject blockObject = new JSONObject();
                    blockObject.put("columnName", fileBlock.getBlockName());
                    blockObject.put("blockNumber", fileBlock.getBlockNumber());
                    blockObject.put("blockSize", fileBlock.getBlockSize());
                    blockObject.put("dataOffset", fileBlock.getDataOffset());
                    blockObject.put("dataLength", fileBlock.getDataLength());
                    jsonArray.put(blockObject);
                }
                jsonObject.put("blocks", jsonArray);
                return jsonObject;
            } catch (JSONException e) {
                throw new IOException(
                        "Unable to serialize file descriptor for "
                                + fileDescriptor.getName(), e);
            }
        }

        /**
         * Convert the given bytes to a file descriptor.
         * 
         * @param descriptorBytes
         * @return
         * @throws IOException
         */
        public static FileDescriptor fromBytes(byte[] descriptorBytes)
                throws IOException {
            try {
                if (descriptorBytes == null) {
                    logger.debug("descriptorBytes is null, returning null");
                    return null;
                }
                JSONTokener tokener =
                        new JSONTokener(new InputStreamReader(
                                new ByteArrayInputStream(descriptorBytes)));
                Object obj = tokener.nextValue();
                logger.debug("obj " + obj);
                FileDescriptor fileDescriptor =
                        FileDescriptorUtils.fromJSON((JSONObject) obj);
                logger.debug("isDeleted flag? {}", fileDescriptor.isDeleted());
                return (!fileDescriptor.isDeleted() ? fileDescriptor : null);
            } catch (JSONException e) {
                e.printStackTrace();
                throw new IOException("Could not get descriptor for file.", e);
            }
        }

        /**
         * Convert the given {@link JSONObject} to a file descriptor.
         * 
         * @param jsonObject
         * @return
         * @throws IOException
         */
        public static FileDescriptor fromJSON(JSONObject jsonObject)
                throws IOException {
            try {
                FileDescriptor fileDescriptor =
                        new FileDescriptor(jsonObject.getString("name"));
                fileDescriptor.setLength(jsonObject.getLong("length"));
                fileDescriptor.setDeleted(jsonObject.getBoolean("deleted"));
                fileDescriptor.setLastModified(jsonObject
                        .getLong("lastModified"));
                fileDescriptor.setLastAccessed(jsonObject
                        .getLong("lastAccessed"));
                fileDescriptor.setBlocks(new LinkedList<FileBlock>());
                JSONArray blockArray = jsonObject.getJSONArray("blocks");
                if (blockArray != null) {
                    for (int index = 0; index < blockArray.length(); index++) {
                        JSONObject blockObject =
                                (JSONObject) blockArray.get(index);
                        FileBlock fileBlock = new FileBlock();
                        fileBlock.setBlockName(blockObject
                                .getString("columnName"));
                        fileBlock.setBlockNumber(blockObject
                                .getInt("blockNumber"));
                        fileBlock.setBlockSize(blockObject.getInt("blockSize"));
                        fileBlock.setDataOffset(blockObject
                                .getInt("dataOffset"));
                        fileBlock.setDataLength(blockObject
                                .getInt("dataLength"));
                        fileDescriptor.addLastBlock(fileBlock);
                    }
                }
                JSONObject debug = toJSON(fileDescriptor);
                logger.debug("fromJSON {}" ,debug.toString(2));
                return fileDescriptor;
            } catch (JSONException e) {
                throw new IOException(
                        "Unable to de-serialize file descriptor from "
                                + jsonObject, e);
            }
        }

        /**
         * Seek to the file block that the given file pointer positions itself
         * on.
         * @param descriptor
         *            the descriptor of the file
         * @param filePointer
         *            the pointer within the file to seek to
         * @return
         */
        public static FileBlock seekBlock(FileDescriptor descriptor,
                long filePointer) {
            long currentPointer = 0;
            for (FileBlock fileBlock : descriptor.getBlocks()) {
                logger.trace("filePointer {} currentPointer {} ", filePointer, currentPointer);
                if (filePointer <= currentPointer) {
                    fileBlock
                            .setDataPosition((int) (filePointer - currentPointer));
                    fileBlock.setBlockOffset(currentPointer);
                    logger.trace("seekBlock returning object fs.getName {} filePointer {}", descriptor.getName(), filePointer);
                    logger.trace("currentPointer {} blocksize {}", currentPointer, descriptor.getBlocks().size());
                    return fileBlock;
                } else {
                    fileBlock.setDataPosition((int)filePointer);
                    fileBlock.setBlockOffset(currentPointer);
                    if (filePointer <= fileBlock.getDataLength()) {
                        logger.info("filePointer {} fileBlock.getDataLength() {} ", filePointer, fileBlock.getDataLength());
                        return fileBlock;
                    }
                }
                currentPointer += fileBlock.getDataLength();
            }
            logger.trace("seekBlock returning null fs.getName {} filePointer {}", descriptor.getName(), filePointer);
            logger.trace("currentPointer {} blocksize {}", currentPointer, descriptor.getBlocks().size());
            return null;
        }
    }

    /**
     * The <code>ColumnOrientedDirectory</code> captures the mapping of the
     * concepts of a directory to a column family in Cassandra. Specifically, it
     * treats each row in the column family as a file underneath the directory.
     * 
     * <p>
     * This class in turn relies on the {@link CassandraClient} for all
     * low-level gets and puts to the Cassandra server. More importantly, it
     * does not require that the {@link CassandraClient} to be familiar with the
     * notion of Lucene directories. Rather, it transparently translates those
     * notions to column families. In so doing, it ends up hiding the Cassandra
     * layer from its consumers.
     * </p>
     */
    public class ColumnOrientedDirectory {
        /**
         * @return the names of the files in this directory
         * @throws IOException
         */
        public String[] getFileNames() throws IOException {
            logger.trace("getFileNames");
            byte[][] keys = cassandraClient.getKeys(systemColumns);
            List<String> fileNames = new ArrayList<String>();
            for (byte[] key : keys) {
                fileNames.add(new String(key));
            }
            return fileNames.toArray(new String[] {});
        }

        /**
         * Return the file descriptor for the file of the given name. If the
         * file cannot be found, then return null, instead of trying to create
         * it.
         * 
         * @param fileName
         *            the name of the file
         * @return the descriptor for the given file
         * @throws IOException
         */
        protected FileDescriptor getFileDescriptor(String fileName)
                throws IOException {
            logger.trace("getFileDescriptor {}", fileName);
            return getFileDescriptor(fileName, false);
        }

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
        protected FileDescriptor getFileDescriptor(String fileName,
                boolean createIfNotFound) throws IOException {
            logger.trace("getFileDescriptor {} with createIfNotFound {}",
                    fileName, createIfNotFound);
            FileDescriptor fileDescriptor =
                    FileDescriptorUtils.fromBytes(cassandraClient.getColumn(
                            fileName.getBytes(), descriptorColumn.getBytes()));
            if (fileDescriptor == null && createIfNotFound) {
                logger.info(
                        "fileDescriptor {} is null, creating a new file descriptor.",
                        fileName);
                fileDescriptor = new FileDescriptor(fileName, getBlockSize());
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
            cassandraClient.setColumns(
                    ByteBufferUtil.bytes(fileDescriptor.getName()), blockMap);
        }
    }

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
            Map<byte[], byte[]> columns =
                    cassandraClient.getColumns(fileDescriptor.getName()
                            .getBytes(), blockNames);
            BlockMap blockMap = new BlockMap();
            blockMap.putAll(columns);
            return blockMap;
        }

    }

    /**
     * The <code>CassandraIndexOutput</code> acts as an output stream for a
     * Cassandra-based file of a given name. In essence, it allows Lucene's
     * low-level data types to be written into that file, using a write-behind
     * caching mechanism.
     * 
     * <p>
     * Specifically, it builds upon the write-behind cache implemented by the
     * {@link BufferedIndexOutput}, by essentially mapping one or more blocks
     * into the underlying buffer. Ergo, it is recommended that the buffer size
     * be a exact multiple of the block size, as that will help to reduce the
     * number of round trips to the Cassandra server.
     * </p>
     */
    public class CassandraIndexOutput extends BufferedIndexOutput {
        // The file descriptor corresponding to this file.
        FileDescriptor fileDescriptor;

        // A map that keeps track of (partial block) fragments. While this is
        // currently not being used, it could be used to de-fragment the file as
        // subsequence writes occur.
        BlockMap fragments;

        // The file block where the file pointer is currently positioned.
        FileBlock currentBlock;

        // A flag indicating whether or not this stream is open.
        private volatile boolean isOpen;

        protected ColumnOrientedFile columnOrientedFile;

        public CassandraIndexOutput(String fileName, int bufferSize)
                throws IOException {
            super(bufferSize);
            logger.trace("fileName = {} bufferSize = {}", fileName, bufferSize);
            fileDescriptor = columnOrientedDirectory.getFileDescriptor(fileName, true);
            currentBlock = fileDescriptor.getFirstBlock();
            columnOrientedFile = new ColumnOrientedFile();
            isOpen = true;
        }

        /**
         * Only close the file if it has not been closed yet.
         */
        @Override
        public void close() throws IOException {
            logger.trace("close ");
            if (isOpen) {
                try {
                    super.close();
                } finally {
                    isOpen = false;
                    if (fragments != null) {
                        fragments.clear();
                    }
                }
            }
        }

        /**
         * @return the current length of the file
         */
        @Override
        public long length() throws IOException {
            logger.trace(" length() ");
            return fileDescriptor.getLength();
        }

        /**
         * Seek to the block corresponding to the given file position, and then
         * move to the exact data position within that block.
         */
        @Override
        public void seek(long position) throws IOException {
            currentBlock = FileDescriptorUtils.seekBlock(fileDescriptor, position);
            if (currentBlock == null) {
                logger.info("currentBlock is set to null with position {} fileDescriptor name {}", position, fileDescriptor.getName());
            }
        }

        /**
         * Map the bytes (that were in the underlying buffer) to their
         * corresponding file blocks based on the current file pointer, and
         * write the blocks through to the column family in the Cassandra
         * server.
         */
        @Override
        protected void flushBuffer(byte[] bytes, int offset, int length)
                throws IOException {
            assert isOpen;
            String debug = String.format(
                    "flushing buffer.. bytes {} offset {} length {}", new String(bytes), 
                    offset, length);            
            logger.trace(debug);
            
            if (length == 0) {
                return;
            }

            BlockMap blocksToFlush = new BlockMap();

            if (currentBlock.getDataPosition() > 0) {
                FileBlock preFragment = (FileBlock) currentBlock.clone();
                preFragment.setDataLength(currentBlock.getDataPosition());
                fileDescriptor.insertBlock(currentBlock, preFragment, false);
            }

            int bytesLeftToWrite = length;
            int bytesAddedByWrite = 0;
            
            do {
                maybeRepositionCurrentBlock();
                int dataLength =
                        (int) Math.min(currentBlock.getBlockSize()
                                - currentBlock.getPositionOffset(),
                                bytesLeftToWrite);
                int currentLength = currentBlock.getDataLength();
                FileBlock nextBlock;
                if (currentBlock.getDataPosition() == 0
                        && dataLength > currentBlock.getDataLength()) {
                    nextBlock = currentBlock;
                    nextBlock.setDataLength(dataLength);
                } else {
                    nextBlock = fileDescriptor.createBlock();
                    nextBlock.setDataLength(dataLength);
                    nextBlock.setDataOffset(currentBlock.getPositionOffset());
                }
                byte[] partialBytes = new byte[dataLength];
                System.arraycopy(bytes, offset, partialBytes, 0, dataLength);
                blocksToFlush.put(nextBlock.getBlockName(), partialBytes);
                nextBlock.setDataPosition(dataLength);
                if (nextBlock != currentBlock) {
                    FileBlock blockToBeRemoved;
                    if (nextBlock.getDataPosition() > 0) {
                        blockToBeRemoved = currentBlock;
                        fileDescriptor.insertBlock(currentBlock, nextBlock,
                                true);
                    } else {
                        blockToBeRemoved = currentBlock;
                        fileDescriptor.insertBlock(currentBlock, nextBlock,
                                false);
                    }
                    for (; blockToBeRemoved != null
                            && blockToBeRemoved.getLastDataOffset() < nextBlock
                                    .getLastDataOffset(); blockToBeRemoved =
                            fileDescriptor.getNextBlock(blockToBeRemoved)) {
                        fileDescriptor.removeBlock(blockToBeRemoved);
                    }
                }
                bytesLeftToWrite -= dataLength;
                offset += dataLength;
                if (fileDescriptor.isLastBlock(nextBlock)) {
                    if (nextBlock != currentBlock) {
                        bytesAddedByWrite += dataLength;
                    } else {
                        bytesAddedByWrite += dataLength - currentLength;
                    }
                }
                currentBlock = nextBlock;
            } while (bytesLeftToWrite > 0);

            if (currentBlock.getDataPosition() < currentBlock.getDataLength()) {
                FileBlock postFragment = (FileBlock) currentBlock.clone();
                postFragment.setDataOffset(currentBlock.getPositionOffset());
                postFragment
                        .setDataLength((int) (currentBlock.getDataLength() - postFragment
                                .getDataOffset()));

                fileDescriptor.insertBlock(currentBlock, postFragment, true);
                currentBlock = postFragment;
                currentBlock.setBlockOffset(currentBlock.getBlockOffset()
                        + currentBlock.getDataPosition());
                currentBlock.setDataPosition(0);
            }

            maybeRepositionCurrentBlock();

            long now = new Date().getTime();
            if (bytesAddedByWrite > 0) {
                fileDescriptor.setLength(fileDescriptor.getLength()
                        + bytesAddedByWrite);
            }
            fileDescriptor.setLastAccessed(now);
            fileDescriptor.setLastModified(now);
            columnOrientedFile.writeFileBlocks(fileDescriptor, blocksToFlush);
        }

        /**
         * In the event the file pointer is currently positioned at the exact
         * end of the data range in the current block, then reposition to the
         * first byte in the ensuing block. Furthermore, if there is no ensuing
         * block, then create a brand-new block, append it to the end of the
         * file, and move into that new block.
         */
        protected void maybeRepositionCurrentBlock() {
            logger.trace("maybeRepositionCurrentBlock");
            if (currentBlock.getDataPosition() == currentBlock.getDataLength()
                    && currentBlock.getPositionOffset() == currentBlock.getBlockSize()) {
                FileBlock nextBlock = fileDescriptor.getNextBlock(currentBlock);
                if (nextBlock == null) {
                    nextBlock = fileDescriptor.createBlock();
                    fileDescriptor.insertBlock(currentBlock, nextBlock, true);
                }
                currentBlock = nextBlock;
            }
        }

        /*
         * 
        @Override
        public void setLength(long length) throws IOException {
            logger.trace("setLength length {}", length);
            super.setLength(length);
        }
        */
        
    }

    /**
     * The <code>CassandraIndexInput</code> acts as a input stream for a
     * Cassandra-based file of a given name. In essence, it allows Lucene's
     * low-level data types to be read from that file, using a read-ahead
     * caching mechanism.
     * 
     * <p>
     * Specifically, it builds upon the read-ahead cache implemented by the
     * {@link BufferedIndexInput}, by essentially mapping one or more blocks
     * into the underlying buffer. Ergo, it is recommended that the buffer size
     * be a exact multiple of the block size, as that will help to reduce the
     * number of round trips to the Cassandra server.
     * </p>
     */
    public class CassandraIndexInput extends BufferedIndexInput {
        // A reference to the descriptor for the file being read from.
        protected final FileDescriptor fileDescriptor;

        // A reference to the current block being read from.
        protected FileBlock currentBlock;

        // The length of the file as determined when it was opened.
        protected long fileLength;

        protected ColumnOrientedFile columnOrientedFile;

        /**
         * Construct a type of {@link IndexInput} that understands how to read
         * from the Cassandra-based file of the given name. It uses a read-ahead
         * buffer under the covers whose size is specified by the given buffer
         * size.
         * 
         * @param fileName
         *            the name of the file to read
         * @param bufferSize
         *            the size of the input buffer
         * @throws IOException
         */
        public CassandraIndexInput(String fileName, int bufferSize)
                throws IOException {

            super("lucene-cassandra-desc", bufferSize);
            logger.trace("initializing CassandraIndexInput {}", fileName);

            fileDescriptor = columnOrientedDirectory.getFileDescriptor(fileName);
            if (fileDescriptor == null) {
                throw new IOException("Unable to locate file " + fileName);
            }
            logger.info(String.format("filename = '%s' bufferSize = '%s'",
                    fileName, bufferSize));
            currentBlock = fileDescriptor.getFirstBlock();
            if (currentBlock == null) {
                logger.error("currentBlock is null for fileName {}", fileName);
            }
            fileLength = fileDescriptor.getLength();
            columnOrientedFile = new ColumnOrientedFile();
        }

        /**
         * There's nothing to close.
         */
        @Override
        public void close() throws IOException {
            logger.trace("close");
        }

        /**
         * @return the cached value of the file length
         */
        @Override
        public long length() {
            logger.info("called length() and returning {}", fileLength);
            return fileLength;
        }

        /**
         * Read the given number (i.e., length) of bytes, by first determining
         * which blocks to retrieve, and then copying over the data from each of
         * those blocks into the given byte array starting at the given offset,
         * one block at a time.
         * 
         * @param bytes
         *            the array to read bytes into
         * @param offset
         *            the offset in the array to start storing bytes
         * @param length
         *            the number of bytes to read
         */
        @Override
        protected void readInternal(byte[] bytes, int offset, int length)
                throws IOException {
            Set<byte[]> blockNames = new TreeSet<byte[]>(BYTE_ARRAY_COMPARATOR);
            List<FileBlock> blocksToBeRead = new ArrayList<FileBlock>();

            int bytesToBeRead = length;

            logger.trace("read internal to bytes with offset {} and length {}",
                    offset, length);
            logger.info("fileDescriptor name = '{}' fileLength = '{}'",
                    fileDescriptor.getName(), fileLength);
            logger.info("fileDescriptor length {} fileDescriptor blockSize {}",
                    fileDescriptor.getLength(), fileDescriptor.getBlocks()
                            .size());

            if (currentBlock == null) {
                //currentBlock = fileDescriptor.getFirstBlock();
                //return;
            }

            do {
                byte[] columnName = currentBlock.getBlockName().getBytes();
                if (!blockNames.contains(columnName)) {
                    logger.info("adding columnName {}", new String(columnName));
                    blockNames.add(columnName);
                }
                blocksToBeRead.add(currentBlock);
                FileBlock nextBlock = fileDescriptor.getNextBlock(currentBlock);
                if (nextBlock == null) {
                    logger.info("next block is null, breaking out of loop");
                    break;
                }
                bytesToBeRead -= currentBlock.getDataLength();
                currentBlock = nextBlock;
            } while (bytesToBeRead > 0);

            logger.trace("blockNames size {}", blockNames.size());

            BlockMap blockMap =
                    columnOrientedFile.readFileBlocks(fileDescriptor,
                            blockNames);
            bytesToBeRead = length;
            for (FileBlock blockToBeRead : blocksToBeRead) {
                logger.info("reading length {}", blockToBeRead.getDataLength());
                for (Map.Entry<byte[], byte[]> columnEntry : blockMap
                        .entrySet()) {
                    String columnName = new String(columnEntry.getKey());
                    byte[] columnValue = columnEntry.getValue();
                    logger.trace("reading columnName {}, columnValue {}",
                            columnName, new String(columnValue));
                    if (columnName.equals(blockToBeRead.getBlockName())) {
                        int bytesToReadFromBlock =
                                (int) Math
                                        .min(bytesToBeRead,
                                                (blockToBeRead.getDataLength() + blockToBeRead
                                                        .getDataPosition()));
                        System.arraycopy(columnValue, 0, bytes, offset,
                                bytesToReadFromBlock);
                        logger.trace("bytes read {}", new String(bytes));
                        bytesToBeRead -= bytesToReadFromBlock;
                        offset += bytesToReadFromBlock;
                        blockToBeRead.setDataPosition(blockToBeRead
                                .getDataPosition() + bytesToReadFromBlock);
                        logger.info(String
                                .format("bytesToBeRead %s offset %s bytesToReadFromBlock %s blockToBeRead.getDataPosition() %s",
                                        bytesToBeRead, offset,
                                        bytesToReadFromBlock,
                                        blockToBeRead.getDataPosition()));
                    }
                }
            }

            logger.info(
                    "currentBlock.getDataPosition() {} currentBlock.getDataLength() {}",
                    currentBlock.getDataPosition(),
                    currentBlock.getDataLength());

            if (currentBlock.getDataPosition() == currentBlock.getDataLength()) {
                FileBlock nextBlock = fileDescriptor.getNextBlock(currentBlock);
                if (nextBlock != null) {
                    currentBlock = nextBlock;
                    logger.trace("nextblock set " + currentBlock);
                }
                logger.info("using block {}", currentBlock.getBlockNumber());
            }
        }

        /**
         * Seek to the block corresponding to the given file position, and then
         * move to the exact data position within that block.
         */
        @Override
        protected void seekInternal(long position) throws IOException {
            logger.info("seeking internal with position {}", position);
            currentBlock =
                    FileDescriptorUtils.seekBlock(fileDescriptor, position);
            if (currentBlock == null) {
                logger.trace("seekInternal with currentBlock set to null, fileDescriptor name {}", fileDescriptor.getName());
            }
        }

        /*
        @Override
        public CassandraIndexInput clone() {
            CassandraIndexInput clone = (CassandraIndexInput) super.clone();
            return clone();
        }
        */
        
        
    }

    /**
     * The <code>CassandraClient</code> encapsulates the low-level interactions
     * of the directory with the (remote) Cassandra server. In particular, it
     * delegates the request through to a Thrift client, which may or may not be
     * framed.
     * 
     * <p>
     * Note that this class is not aware of the notions of directories and files
     * described above. Instead, it simply provides basic operations, such as
     * those that get/set columns/keys. This separation of concern of the
     * concepts of Cassandra and Lucene serves to not only keep the design
     * simple, but also readable (hopefully).
     * </p>
     */
    public class CassandraClient {
        // The underlying thrift client to delegate requests to.
        protected Cassandra.Client thriftClient;

        /**
         * Construct a Cassandra client that knows how to get/set rows/columns
         * from the given keyspace and column family, residing in the given
         * Cassandra server.
         * 
         * http://stackoverflow.com/questions/7018273/need-explanation-of-
         * transferring-binary-data-using-thrift-rpc
         * 
         * @param host
         *            the host where the Cassandra Thrift server is located
         * @param port
         *            the port where the Cassandra Thrift server is listening
         * @param framed
         *            a flag indicating whether or not to use a framed transport
         * @throws IOException
         */
        public CassandraClient(String host, int port, boolean framed)
                throws IOException {
            logger.trace(String
                    .format("initialize cassandra client with host %s port %s framed %s",
                            host, port, framed));
            TSocket socket = new TSocket(host, port);
            TTransport transport =
                    framed ? new TFramedTransport(socket) : socket;
            try {
                transport.open();
                thriftClient =
                        new Cassandra.Client(new TBinaryProtocol(transport));
                Map<String, String> credentials = new HashMap<String, String>();
                credentials.put(IAuthenticator.USERNAME_KEY, "");
                credentials.put(IAuthenticator.PASSWORD_KEY, "");
                List<KsDef> keyspaces = thriftClient.describe_keyspaces();
                boolean createKeyspace = true;
                boolean createColumnFamily = true;

                for (KsDef ks : keyspaces) {
                    if (ks.name.equals(keyspace)) {
                        createKeyspace = false;
                        for (CfDef cf : ks.getCf_defs()) {
                            if (cf.getName().equals(columnFamily)) {
                                createColumnFamily = false;
                            }
                        }
                        break;
                    }
                }

                if (createKeyspace) {
                    List<CfDef> cfDefs = new ArrayList<CfDef>();
                    cfDefs.add(new CfDef(keyspace, columnFamily));
                    KsDef ksDef =
                            new KsDef(
                                    keyspace,
                                    "org.apache.cassandra.locator.SimpleStrategy",
                                    cfDefs);
                    ksDef.putToStrategy_options("replication_factor", "1");
                    thriftClient.system_add_keyspace(ksDef);
                }
                thriftClient.set_keyspace(keyspace);
                try {
                    if (createColumnFamily) {
                        CfDef cfDef = new CfDef(keyspace, columnFamily);
                        thriftClient.system_add_column_family(cfDef);
                    }
                } catch (InvalidRequestException e) {
                    e.printStackTrace();
                }
                thriftClient.login(new AuthenticationRequest(credentials));
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Unable to open connection to keyspace "
                        + keyspace, e);
            }
        }

        /**
         * Return the keys that define the given column names.
         * 
         * @param columnNames
         *            the names of the columns
         * @return the rows that contain those columns
         * @throws IOException
         */
        public byte[][] getKeys(List<byte[]> columnNames) throws IOException {
            logger.trace("getKeys");
            try {
                List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
                for (byte[] b : columnNames) {
                    converter.add(ByteBuffer.wrap(b));
                }
                List<KeySlice> keySlices =
                        thriftClient
                                .get_range_slices(
                                        new ColumnParent()
                                                .setColumn_family(columnFamily),
                                        new SlicePredicate()
                                                .setColumn_names(converter),
                                        new KeyRange().setStart_key(
                                                "".getBytes()).setEnd_key(
                                                "".getBytes()),
                                        ConsistencyLevel.ONE);
                List<byte[]> keys = new ArrayList<byte[]>();
                for (KeySlice keySlice : keySlices) {
                    List<ColumnOrSuperColumn> coscs = keySlice.getColumns();
                    if (coscs != null && coscs.size() == 1) {
                        ColumnOrSuperColumn cosc = coscs.get(0);
                        Column column = cosc.getColumn();
                        FileDescriptor fileDescriptor =
                                FileDescriptorUtils
                                        .fromBytes(column.getValue());
                        if (fileDescriptor == null
                                || fileDescriptor.isDeleted()) {
                            continue;
                        }
                        keys.add(ByteBufferUtil.getArray(keySlice.key));
                    }
                }
                return keys.toArray(new byte[][] {});
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Unable to list all files in " + keyspace);
            }
        }

        /**
         * Get the given set of columns for the row specified by the given key.
         * 
         * @param key
         *            the key to the row to read from
         * @param columnNames
         *            the names of the columns to fetch
         * @return the values for those columns in that row
         * @throws IOException
         */
        public Map<byte[], byte[]> getColumns(byte[] key,
                Set<byte[]> columnNames) throws IOException {
            logger.trace("getColumns key {}", new String(key));
            try {
                List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
                for (byte[] b : columnNames) {
                    converter.add(ByteBuffer.wrap(b));
                }
                List<ColumnOrSuperColumn> coscs =
                        thriftClient
                                .get_slice(ByteBuffer.wrap(key),
                                        new ColumnParent(columnFamily),
                                        new SlicePredicate()
                                                .setColumn_names(converter),
                                        ConsistencyLevel.ONE);
                Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
                for (ColumnOrSuperColumn cosc : coscs) {
                    Column column = cosc.getColumn();
                    columns.put(column.getName(), column.getValue());
                }
                return columns;
            } catch (Exception e) {
                throw new IOException("Could not read from columns for file "
                        + key, e);
            }
        }

        /**
         * Get the given column for the row specified by the given key.
         * 
         * @param key
         *            the key to the row to read from
         * @param columnName
         *            the name of the column to fetch
         * @return the value for that column in this row
         * @throws IOException
         */
        public byte[] getColumn(byte[] fileName, byte[] columnName)
                throws IOException {
            logger.trace("getColumn fileName {} columnName {} ", new String(
                    fileName), new String(columnName));
            try {
                List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
                converter.add(ByteBuffer.wrap(columnName));
                List<ColumnOrSuperColumn> coscs =
                        thriftClient
                                .get_slice(
                                        ByteBuffer.wrap(fileName),
                                        new ColumnParent()
                                                .setColumn_family(columnFamily),
                                        new SlicePredicate()
                                                .setColumn_names(converter),
                                        ConsistencyLevel.ONE);
                if (!coscs.isEmpty()) {
                    ColumnOrSuperColumn cosc = coscs.get(0);
                    Column column = cosc.getColumn();
                    return column.getValue();
                }
                return null;
            } catch (Exception e) {
                throw new IOException("Unable to read file descriptor for "
                        + fileName, e);
            }
        }

        /**
         * Set the values for the given columns in the given row.
         * 
         * @param key
         *            the key to the row being written to
         * @param columnValues
         *            the values for the columns being updated
         * @throws IOException
         */
        protected void setColumns(ByteBuffer key,
                Map<byte[], byte[]> columnValues) throws IOException {
            logger.trace("setColumns");
            Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                    new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

            Map<String, List<Mutation>> cfMutation =
                    new HashMap<String, List<Mutation>>();
            // System.out.println("key => " + ByteBufferUtil.string(key));
            logger.debug("columnValues size => {}", columnValues.size());
            mutationMap.put(key, cfMutation);

            List<Mutation> mutationList = new ArrayList<Mutation>();
            cfMutation.put(columnFamily, mutationList);

            if (columnValues == null || columnValues.size() == 0) {
                Mutation mutation = new Mutation();
                Deletion deletion = new Deletion();
                // try to delete column in cassandra 1.0.8
                // deletion.predicate = new SlicePredicate();
                // deletion.predicate.column_names =
                deletion.setTimestamp(System.currentTimeMillis());
                /**
                 * Currently, we cannot delete rows from a column family. This
                 * issue is being tracked at
                 * https://issues.apache.org/jira/browse/CASSANDRA-293. When
                 * that issue that resolved, we may at that time choose to
                 * revive the code shown below.
                 * 
                 * deletion.setPredicate(new SlicePredicate().setSlice_range(new
                 * SliceRange(new byte[] {}, new byte[] {}, false,
                 * Integer.MAX_VALUE)));
                 */
                mutation.setDeletion(deletion);
                mutationList.add(mutation);

            } else {
                for (Map.Entry<byte[], byte[]> columnValue : columnValues
                        .entrySet()) {
                    Mutation mutation = new Mutation();
                    byte[] column = columnValue.getKey(), value =
                            columnValue.getValue();
                    logger.debug("columnName => {} columnValue => {}",
                            new String(column, "UTF-8"), new String(value,
                                    "UTF-8"));
                    boolean isDelete = false;
                    // yet.
                    /*
                     * if (value != null) { FileDescriptor fd =
                     * FileDescriptorUtils.fromBytes(value); if (fd == null ||
                     * fd.isDeleted()) { System.out.println("entered!");
                     * isDelete = true; } }
                     */
                    if (value == null || isDelete) {
                        logger.debug("value is " + value);
                        Deletion deletion = new Deletion();
                        deletion.setTimestamp(System.currentTimeMillis());
                        // try to delete column in cassandra 1.0.8
                        deletion.predicate = new SlicePredicate();
                        deletion.predicate.column_names =
                                Arrays.asList(ByteBuffer.wrap(column));

                        if (column != null) {
                            List<ByteBuffer> converter =
                                    new ArrayList<ByteBuffer>();
                            converter.add(ByteBuffer.wrap(column));
                            // deletion.setPredicate(new
                            // SlicePredicate().setColumn_names(Arrays
                            // .asList(new byte[][] {column})));
                            deletion.setPredicate(new SlicePredicate()
                                    .setColumn_names(converter));
                        } else {
                            /*
                             * deletion.setPredicate(new SlicePredicate()
                             * .setSlice_range(new SliceRange(new byte[] {}, new
                             * byte[] {}, false, Integer.MAX_VALUE)));
                             */
                            deletion.setPredicate(new SlicePredicate()
                                    .setSlice_range(new SliceRange(
                                            ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                            ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                            false, Integer.MAX_VALUE)));

                        }

                        mutation.setDeletion(deletion);

                    } else {
                        logger.debug("value is not null");
                        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();

                        Column nColumn = new Column();
                        nColumn.setName(ByteBuffer.wrap(column));
                        nColumn.setValue(ByteBuffer.wrap(value));
                        nColumn.setTimestamp(System.currentTimeMillis());
                        cosc.setColumn(nColumn);

                        mutation.setColumn_or_supercolumn(cosc);
                    }

                    mutationList.add(mutation);
                }
            }
            try {
                thriftClient.batch_mutate(mutationMap, ConsistencyLevel.ONE);
            } catch (Exception e) {
                throw new IOException("Unable to mutate columns for file "
                        + key, e);
            }
        }
    }
}
