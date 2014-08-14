package org.apache.lucene.cassandra;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.cassandra.nio.CassandraFileSystem;
import org.apache.lucene.cassandra.nio.CassandraFileSystemProvider;
import org.apache.lucene.store.IOContext;
import org.apache.monitor.Counter;
import org.apache.monitor.JmxMonitor;
import org.apache.monitor.MonitorType;
import org.apache.monitor.OpentrackerClientMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ACassandraFile implements File, Closeable, MonitorType, Path {

    private String name = null;

    private String keyspace = null;

    private String columnFamily = null;

    private String cassandraDirectory = "";

    private IOContext mode = null;

    private long length;

    // private boolean isDelete = false;
    private long lastModified = -1;

    private int blockSize;

    private FileBlock currentBlock = null;

    private static Logger logger = LoggerFactory
            .getLogger(ACassandraFile.class);

    private CassandraClient cassandraClient = null;

    private ColumnOrientedDirectory columnOrientedDirectory;

    private FileDescriptor fd = null;

    private ColumnOrientedFile columnOrientedFile = null;

    private boolean isModeMerge = false;

    private OpentrackerClientMonitor monitor;

    public static int getDNCount = 0;

    public static long getDNTime = 0;
    
    private volatile transient Path filePath;
    
    private CassandraFileSystem fs;
    
    private CassandraFileSystemProvider provider = new CassandraFileSystemProvider();
    
    @Override

    public File get(File directory, String name) {
        getDNCount++;
        long ms = System.currentTimeMillis();

        try {
            ACassandraFile acf =
                    new ACassandraFile(directory.getCanonicalPath(), name,
                            IOContext.DEFAULT, true, "lucene0", "index0", 16384);
            getDNTime += System.currentTimeMillis() - ms;
            return acf;
        } catch (IOException e) {
            logger.error("File get didnt work", e);
            e.printStackTrace();
            throw new RuntimeException("File get didnt work");
        }
    }

    @Override
    public File get(String canonicalPath) {
        return new ACassandraFile(canonicalPath);
    }

    public static int getRAFCount = 0;

    public static long getRAFTime = 0;
    @Override
    public RandomAccessFile getRandomAccessFile(File fullFile,
            String permissions) throws FileNotFoundException {
        getRAFCount++;
        long ms = System.currentTimeMillis();

        ACassandraRandomAccessFile araf = new ACassandraRandomAccessFile(fullFile, IOContext.DEFAULT,
                true, "lucene0", "index0", 16384);
        getRAFTime += System.currentTimeMillis() - ms;
        return araf;
    }

    public ACassandraFile(String canonicalPath) {
        String[] tokens = canonicalPath.split("\\/(?=[^\\/]+$)");
        
        String directory = tokens[0];
        String name = tokens[1];
        
        if (directory == null) {
            // always start with root.
            directory = "/";
            this.name = directory + name;
        } else {
            if (!directory.equals("/")) {
                directory += "/";
                this.name = directory + "/" + name;
            } else {
                this.name = directory + name;
            }
        }
        this.name = this.name.replaceAll("//", "/");
        this.mode = IOContext.DEFAULT;
        this.blockSize = 16384;
        this.keyspace = "lucene0";
        this.columnFamily = "index0";
        this.cassandraDirectory = directory;
        //logger.info("cassandraDirectory {} name {}", cassandraDirectory, name);
        this.fs = new CassandraFileSystem(provider, cassandraDirectory);
        boolean readOnly = true;
        monitor = JmxMonitor.getInstance().getCassandraMonitor(this);
        try {
            cassandraClient =
                    new CassandraClient("localhost", 9160, true, keyspace,
                            columnFamily, blockSize);
            this.columnOrientedDirectory =
                    new ColumnOrientedDirectory(cassandraClient, blockSize);
            this.columnOrientedFile = new ColumnOrientedFile(cassandraClient);
            if (mode == null
                    || mode.context == IOContext.Context.DEFAULT
                    || mode.context == IOContext.Context.FLUSH
                    || mode.context == IOContext.Context.MERGE
                    || (mode.context == IOContext.Context.READ && name
                            .equals("segments.gen"))) {
                // when mode == null when writing write.lock
                this.fd =
                        this.columnOrientedDirectory.getFileDescriptor(
                                this.name, true);
                readOnly = false;
            } else if (mode.context == IOContext.Context.READ) {
                this.fd =
                        this.columnOrientedDirectory
                                .getFileDescriptor(this.name);
                readOnly = true;
            }
            if (fd == null) {
                if (!readOnly) {
                    throw new IOException(
                            "fd is null, unable to retrieve file " + this.name);
                }
            } else {
                length = fd.getLength();
                currentBlock = fd.getFirstBlock();
            }
            if (mode.context == IOContext.Context.MERGE) {
                isModeMerge = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static int getACFCount = 0;

    public static long getACFTime = 0;

    public ACassandraFile(String directory, String name, IOContext mode,
            boolean frameMode, String keyspace, String columnFamily,
            int blockSize) {
        getACFCount++;
        long ms = System.currentTimeMillis();
        logger.trace("ACassandraFile String directory {}, String name {}",
                directory, name);
        logger.trace("ACassandraFile IOContext mode {}, boolean frameMode {}",
                mode, frameMode);
        logger.trace(
                "ACassandraFile String keyspace {}, String columnFamily {}",
                keyspace, columnFamily);
        if (directory == null) {
            directory = cassandraDirectory;
            this.name = directory + name;
        } else {
            if (!directory.equals("/")) {
                this.name = directory + "/" + name;
            } else {
                this.name = directory + name;
            }
        }
        this.name = this.name.replaceAll("//", "/");
        this.mode = mode;
        this.blockSize = blockSize;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.cassandraDirectory = directory;
        //logger.info("cassandraDirectory {} name {}", cassandraDirectory, name);
        this.fs = new CassandraFileSystem(provider, cassandraDirectory);
        boolean readOnly = true;
        monitor = JmxMonitor.getInstance().getCassandraMonitor(this);
        try {
            cassandraClient =
                    new CassandraClient("localhost", 9160, frameMode, keyspace,
                            columnFamily, blockSize);
            this.columnOrientedDirectory =
                    new ColumnOrientedDirectory(cassandraClient, blockSize);
            this.columnOrientedFile = new ColumnOrientedFile(cassandraClient);
            if (mode == null
                    || mode.context == IOContext.Context.DEFAULT
                    || mode.context == IOContext.Context.FLUSH
                    || mode.context == IOContext.Context.MERGE
                    || (mode.context == IOContext.Context.READ && name
                            .equals("segments.gen"))) {
                // when mode == null when writing write.lock
                this.fd =
                        this.columnOrientedDirectory.getFileDescriptor(
                                this.name, true);
                readOnly = false;
            } else if (mode.context == IOContext.Context.READ) {
                this.fd =
                        this.columnOrientedDirectory
                                .getFileDescriptor(this.name);
                readOnly = true;
            }
            if (fd == null) {
                if (!readOnly) {
                    throw new IOException(
                            "fd is null, unable to retrieve file " + this.name);
                }
            } else {
                length = fd.getLength();
                currentBlock = fd.getFirstBlock();
            }
            if (mode.context == IOContext.Context.MERGE) {
                isModeMerge = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.trace("done {}", this.name);
        getACFTime += System.currentTimeMillis() - ms;

    }
    
    // simple way to initializing ACassandraFile, the real implementation will probably
    // have to comply to the java.io.File(File parent, String child)
    public ACassandraFile(File parent, String child) {
        
        this(parent.getAbsolutePath(), child, IOContext.DEFAULT, true, "lucene0", "index0", 16384);
        
        if (child == null) {
            throw new NullPointerException();
        }
    }

    /**
     * check if file exists in cassandra by checking if row exists.
     * 
     * 
     * @return if file descriptor exists.
     */

    public static int existsCount = 0;

    public static long existsTime = 0;

    public boolean exists() {
        existsCount++;
        long ms = System.currentTimeMillis();
        FileDescriptor descriptor = null;
        boolean isExists = false;
        try {
            if (columnOrientedDirectory == null)
                return true;
            logger.trace("columnOrientedDirectory: {} ",
                    columnOrientedDirectory);
            logger.trace("name: {} ", name);
            descriptor = columnOrientedDirectory.getFileDescriptor(name);
        } catch (IOException e) {
            isExists = false;
        }
        if (descriptor != null) {
            isExists = true;
        }
        logger.trace("called exists {} for file {}", isExists, name);
        existsTime += System.currentTimeMillis() - ms;
        return isExists;
    }

    public boolean isDirectory() {
        logger.trace("called isDirectory always returning true");
        return true;
    }

    public static int listCount = 0;

    public static long listTime = 0;

    public String[] list(java.io.FilenameFilter filenameFilter) {
        listCount++;
        long ms = System.currentTimeMillis();

        String[] files = {};
        try {
            if (columnOrientedDirectory == null) {
                return files;
            }
            files = columnOrientedDirectory.getFileNames();
            logger.info("files length " + files.length);
        } catch (IOException e) {
            logger.error("unable to list ", e);
        }
        listTime += System.currentTimeMillis() - ms;
        return files;
    }

    public static int emptyListCount = 0;

    public static long emptyListTime = 0;

    public String[] list() {
        emptyListCount++;
        long ms = System.currentTimeMillis();
        String[] files = {};
        try {
            files = columnOrientedDirectory.getFileNames();
        } catch (IOException e) {
            logger.error("unable to list ", e);
        }
        emptyListTime += System.currentTimeMillis() - ms;
        return files;
    }

    public String getCanonicalPath() throws IOException {
        /*
        if (cassandraDirectory == null && name != null) {
            logger.info(
                    "called getCanonicalPath returning default / for file {}",
                    name);
            return name;
        }
        logger.info("called getCanonicalPath returning {} for file {}",
                cassandraDirectory, name);
        return cassandraDirectory;
        */
        return name;
    }

    public long length() {
        return length;
    }

    // delete this file in cassandra and return true if it deleted , any thing
    // else, return false;
    public static int deleteCount = 0;

    public static long deleteTime = 0;

    public boolean delete() {
        deleteCount++;
        long ms = System.currentTimeMillis();

        logger.trace("deleting file {}", name);
        if (fd != null) {
            try {
                fd.setDeleted(true);
                columnOrientedDirectory.setFileDescriptor(fd);
                cassandraClient.setColumns(ByteBufferUtil.bytes(fd.getName()),
                        null);
            } catch (IOException e) {
                logger.error("unable to delete file " + name, e);
                deleteTime += System.currentTimeMillis() - ms;

                return false;
            }
        }
        deleteTime += System.currentTimeMillis() - ms;

        return true;
    }

    // create directory in cassandra.
    public boolean mkdirs() {
        return true;
    }

    public String getPath() {
        if (cassandraDirectory == null) {
            return new String("/").concat(name);
        }
        return name;
    }

    public long lastModified() {
        return lastModified;
    }

    public static int createCount = 0;

    public static long createTime = 0;

    public boolean createNewFile() throws IOException {
        createCount++;
        long ms = System.currentTimeMillis();
        logger.trace("creating {}", name);
        try {
            FileDescriptor fd =
                    columnOrientedDirectory.getFileDescriptor(name, true);
            fd.setLastModified(System.currentTimeMillis());
            columnOrientedDirectory.setFileDescriptor(fd);
        } catch (Exception e) {
            logger.error("unable to create a new file " + name, e);
            throw new IOException("unable to create a new file " + name);
        }
        createTime += System.currentTimeMillis() - ms;
        return true;
    }

    public String getAbsolutePath() {
        logger.trace("called getAbsolutePath ");
        if (cassandraDirectory == null) {
            return new String("/").concat(name);
        }
        return name;
    }

    /**
     * in python talk it would be something like file.write(b[off:off+len])) so
     * the byte array , b and take the data from specified offset and until
     * offset + length. then write this length of data into the file
     * 
     * Writes <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this file. overwrite.
     * 
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public static int writeCount = 0;
    public static long writeTime = 0;

    public void write(byte[] b, int off, int len) throws IOException {
        long ms = System.currentTimeMillis();
        writeCount++;
        if (b == null) {
            throw new NullPointerException("array b is null");
        }

        if (off < 0) {
            throw new IndexOutOfBoundsException(
                    "offset for the file which is going to be written must not be negative");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException(
                    "byte array length must not be negative");
        }
        if (len > b.length - off) {
            throw new IndexOutOfBoundsException(
                    "write length must not greater than buffer length minus buffer offset");
        }

        if (len == 0) {
            logger.trace("len is 0 , nothing to flush");
            return;
        }

        // if mode is merge
        if (isModeMerge) {
            monitor.addCounter(Counter.METRIC_TOTAL_MODE_MERGE, len);
            monitor.setCounter(Counter.METRIC_MODE_MERGE, len);
        }

        String debug =
                String.format("flushing buffer.. bytes %s offset %s length %s",
                        Util.debugBytesToHex(b), off, len);
        logger.trace(debug);

        BlockMap blocksToFlush = new BlockMap();

        if (currentBlock.getDataPosition() > 0) {
            logger.trace("creating prefragment");
            FileBlock preFragment = (FileBlock) currentBlock.clone();
            preFragment.setDataLength(currentBlock.getDataPosition());
            fd.insertBlock(currentBlock, preFragment, false);
        }

        int bytesLeftToWrite = len;
        int bytesAddedByWrite = 0;

        do {
            maybeRepositionCurrentBlock();
            // minimum of data to copy into cassandra.
            int dataLength =
                    (int) Math.min(
                            currentBlock.getBlockSize()
                                    - currentBlock.getPositionOffset(),
                            bytesLeftToWrite);
            // current file block data length.
            int currentLength = currentBlock.getDataLength();
            FileBlock nextBlock;
            if (currentBlock.getDataPosition() == 0
                    && dataLength > currentBlock.getDataLength()) {
                // no need create a new block
                nextBlock = currentBlock;
                nextBlock.setDataLength(dataLength);
                debug =
                        String.format(
                                "1 setting block %s for file %s with dataoffset %s",
                                currentBlock.getBlockName(), name,
                                currentBlock.getPositionOffset());
                logger.trace(debug);
            } else {
                // create a new block
                nextBlock = fd.createBlock();
                nextBlock.setDataLength(dataLength);
                nextBlock.setDataOffset(currentBlock.getPositionOffset());
                debug =
                        String.format(
                                "1 setting block %s for file %s with dataoffset %s",
                                currentBlock.getBlockName(), name,
                                currentBlock.getPositionOffset());
                logger.trace(debug);
            }
            byte[] partialBytes = new byte[dataLength];
            System.arraycopy(b, off, partialBytes, 0, dataLength);
            blocksToFlush.put(nextBlock.getBlockName(), partialBytes);
            logger.trace("added block {} with length {} to flush ",
                    nextBlock.getBlockName(), partialBytes.length);
            nextBlock.setDataPosition(dataLength);
            if (nextBlock != currentBlock) {
                FileBlock blockToBeRemoved;
                if (nextBlock.getDataPosition() > 0) {
                    blockToBeRemoved = currentBlock;
                    // add nextBlock into a position of the currentblock + 1.
                    // if currentblock position is 5, so nextblock position is 6
                    fd.insertBlock(currentBlock, nextBlock, true);
                } else {
                    // add nextBlock into a position of the currentblock - 1.
                    // if currentblock position is 5, so nextblock position is 4
                    blockToBeRemoved = currentBlock;
                    fd.insertBlock(currentBlock, nextBlock, false);
                }
                for (; blockToBeRemoved != null
                        && blockToBeRemoved.getLastDataOffset() < nextBlock
                                .getLastDataOffset(); blockToBeRemoved =
                        fd.getNextBlock(blockToBeRemoved)) {
                    fd.removeBlock(blockToBeRemoved);
                }
            }
            bytesLeftToWrite -= dataLength;
            off += dataLength;
            if (fd.isLastBlock(nextBlock)) {
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
            debug =
                    String.format(
                            "2 setting block %s for file %s with dataoffset %s",
                            currentBlock.getBlockName(), name,
                            currentBlock.getPositionOffset());
            logger.trace(debug);
            postFragment
                    .setDataLength((int) (currentBlock.getDataLength() - postFragment
                            .getDataOffset()));

            fd.insertBlock(currentBlock, postFragment, true);
            currentBlock = postFragment;
            currentBlock.setBlockOffset(currentBlock.getBlockOffset()
                    + currentBlock.getDataPosition());
            currentBlock.setDataPosition(0);
        }

        maybeRepositionCurrentBlock();

        long now = new java.util.Date().getTime();
        if (bytesAddedByWrite > 0) {
            fd.setLength(fd.getLength() + bytesAddedByWrite);
        }
        fd.setLastAccessed(now);
        fd.setLastModified(now);
        logger.trace("file descriptor {}", FileDescriptorUtils.toString(fd));
        logger.trace("blocksToFlush size {}", blocksToFlush.size());
        if (blocksToFlush.size() > 1) {
            for (Entry<byte[], byte[]> entry : blocksToFlush.entrySet()) {
                logger.trace("flushing block {} ", new String(entry.getKey()));
            }
        }
        columnOrientedFile.writeFileBlocks(fd, blocksToFlush);
        if (bytesLeftToWrite > 0) {
            logger.error("did not write fully as expected, remaining {}",
                    bytesLeftToWrite);
        }
        writeTime += System.currentTimeMillis() - ms;

    }

    /**
     * In the event the file pointer is currently positioned at the exact end of
     * the data range in the current block, then reposition to the first byte in
     * the ensuing block. Furthermore, if there is no ensuing block, then create
     * a brand-new block, append it to the end of the file, and move into that
     * new block.
     */
    private void maybeRepositionCurrentBlock() {
        logger.trace("maybeRepositionCurrentBlock");
        // this if means use up all the blocksize allocated.
        logger.trace("dataPos {} dataLength {}",
                currentBlock.getDataPosition(), currentBlock.getDataLength());
        logger.trace("positionOffset {} blockSize {}",
                currentBlock.getPositionOffset(), currentBlock.getBlockSize());
        if (currentBlock.getDataPosition() == currentBlock.getDataLength()
                && currentBlock.getPositionOffset() == currentBlock
                        .getBlockSize()) {
            // example if current block is BLOCK-2, nextblock is BLOCK-3
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock == null) {
                // do not have next block to the current block, which mean the
                // current lbock is the last block,
                // and then create a new one.
                nextBlock = fd.createBlock();
                logger.trace("currentblock number {} new nextblock number {}",
                        currentBlock.getBlockNumber(),
                        nextBlock.getBlockNumber());
                fd.insertBlock(currentBlock, nextBlock, true);
            }
            // switch currentblock into new nextblock.
            currentBlock = nextBlock;
        }
    }

    /**
     * Sets the file-pointer offset, measured from the beginning of this file,
     * at which the next read or write occurs. The offset may be set beyond the
     * end of the file. Setting the offset beyond the end of the file does not
     * change the file length. The file length will change only by writing after
     * the offset has been set beyond the end of the file.
     * 
     * @param pos
     *            the offset position, measured in bytes from the beginning of
     *            the file, at which to set the file pointer.
     * @exception IOException
     *                if <code>pos</code> is less than <code>0</code> or if an
     *                I/O error occurs.
     */
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IOException("position cannot be negative");
        }
        if (pos > 2147483647) {
            logger.error("seek {} out of the bound 2147483647", pos);
        }
        // logger.trace("called seek {}", pos);
        currentBlock = FileDescriptorUtils.seekBlock(fd, pos);
        if (currentBlock == null) {
            throw new IOException("currentBlock is null");
        }
        logger.trace("currentblock is now {}", currentBlock.getBlockName());
        logger.trace("blockoffset is now {}", currentBlock.getBlockOffset());
        logger.trace("dataposition is now {}", currentBlock.getDataPosition());
        logger.trace("dataoffset is now {}", currentBlock.getDataOffset());
        logger.trace("blocknumber is now {}", currentBlock.getBlockNumber());
        logger.trace("blockSize is now {}", currentBlock.getBlockSize());
        logger.trace("data length is now {}", currentBlock.getDataLength());
        logger.trace("last data offset is now {}",
                currentBlock.getLastDataOffset());
        logger.trace("positionOffset is now {}",
                currentBlock.getPositionOffset());
    }

    /**
     * Sets the length of this file.
     * 
     * <p>
     * If the present length of the file as returned by the <code>length</code>
     * method is greater than the <code>newLength</code> argument then the file
     * will be truncated. In this case, if the file offset as returned by the
     * <code>getFilePointer</code> method is greater than <code>newLength</code>
     * then after this method returns the offset will be equal to
     * <code>newLength</code>.
     * 
     * <p>
     * If the present length of the file as returned by the <code>length</code>
     * method is smaller than the <code>newLength</code> argument then the file
     * will be extended. In this case, the contents of the extended portion of
     * the file are not defined.
     * 
     * @param newLength
     *            The desired length of the file
     * @exception IOException
     *                If an I/O error occurs
     * @since 1.2
     */
    public void setLength(long newLength) {
        logger.trace("called setLength {}", length);

        if (length() > newLength) {
            // TODO this file is truncated
        } else if (length() < newLength) {
            // TODO extend this file.
        }
    }

    /**
     * Reads up to <code>len</code> bytes of data from this file into an array
     * of bytes. This method blocks until at least one byte of input is
     * available.
     * <p>
     * Although <code>RandomAccessFile</code> is not a subclass of
     * <code>InputStream</code>, this method behaves in exactly the same way as
     * the {@link InputStream#read(byte[], int, int)} method of
     * <code>InputStream</code>.
     * 
     * @param b
     *            the buffer into which the data is read.
     * @param off
     *            the start offset in array <code>b</code> at which the data is
     *            written.
     * @param len
     *            the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or
     *         <code>-1</code> if there is no more data because the end of the
     *         file has been reached.
     * @exception IOException
     *                If the first byte cannot be read for any reason other than
     *                end of file, or if the random access file has been closed,
     *                or if some other I/O error occurs.
     * @exception NullPointerException
     *                If <code>b</code> is <code>null</code>.
     * @exception IndexOutOfBoundsException
     *                If <code>off</code> is negative, <code>len</code> is
     *                negative, or <code>len</code> is greater than
     *                <code>b.length - off</code>
     */
    public int read(byte[] b, int off, int len) throws IOException {
        String debug =
                String.format("buffer '%s' offset %s len %s",
                        Util.debugBytesToHex(b), off, len);
        logger.trace(debug);

        if (b == null) {
            throw new NullPointerException("array b is null");
        }

        if (off < 0) {
            throw new IndexOutOfBoundsException(
                    "buffer offset must not be negative");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException(
                    "read length must not be negative");
        }
        if (len > b.length - off) {
            throw new IndexOutOfBoundsException(
                    "read length must not greater than buffer length minus buffer offset");
        }

        if (currentBlock.getDataPosition() == -1) {
            return -1;
        }

        if (len == 0) {
            return 0;
        }

        // contain unique block name to be read.
        // block-0, block-1 ... block-N
        Set<byte[]> blockNames =
                new TreeSet<byte[]>(BlockMap.BYTE_ARRAY_COMPARATOR);
        List<FileBlock> blocksToBeRead = new ArrayList<FileBlock>();

        int bytesToBeRead = len;

        // logger.trace("fileDescriptor name = '{}' current file length = '{}'",
        // fd.getName(), length);
        // logger.trace("fileDescriptor length {} fileDescriptor total blocks {}",
        // fd.getLength(), fd.getBlocks().size());

        boolean initialStartBlock = true;

        do {
            byte[] columnName = currentBlock.getBlockName().getBytes();
            if (!blockNames.contains(columnName)) {
                logger.trace("adding columnName {} dataPosition {}",
                        new String(columnName), currentBlock.getDataPosition());
                blockNames.add(columnName);
            }
            blocksToBeRead.add(currentBlock);
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock == null) {
                // logger.trace("next block is null, breaking out of loop");
                break;
            }
            if (currentBlock.getDataPosition() >= currentBlock.getDataLength()) {
                logger.warn(
                        "dataPosition {} greater than or equal to dataLength {}",
                        currentBlock.getDataPosition(),
                        currentBlock.getDataLength());
            }
            if (initialStartBlock) {
                bytesToBeRead -=
                        (currentBlock.getDataLength() - currentBlock
                                .getDataPosition());
            } else {
                bytesToBeRead -= currentBlock.getDataLength();
            }
            if (bytesToBeRead < 0) {
                logger.trace("bytesToBeRead {} currentBlock name {}",
                        bytesToBeRead, currentBlock.getBlockName());
            } else {
                currentBlock = nextBlock;
            }
            initialStartBlock = false;
        } while (bytesToBeRead > 0);

        // logger.trace("blockNames size {} blocksToBeRead size {}",
        // blockNames.size(), blocksToBeRead.size());

        BlockMap blockMap = null;

        try {
            // get the row specified by the fd and read all values inside the
            // column specified by column name blockNames.
            blockMap = columnOrientedFile.readFileBlocks(fd, blockNames);
        } catch (Exception e) {
            throw new IOException("cannot read column from cassandra.");
        }

        bytesToBeRead = len;
        int totalRead = 0;
        boolean resetPosition = false;
        for (FileBlock blockToBeRead : blocksToBeRead) {
            // logger.trace("reading fileblock {} of its length {}",
            // blockToBeRead.getBlockName(), blockToBeRead.getDataLength());
            for (Map.Entry<byte[], byte[]> columnEntry : blockMap.entrySet()) {
                String columnName = new String(columnEntry.getKey());
                byte[] columnValue = columnEntry.getValue();
                if (columnName.equals(blockToBeRead.getBlockName())) {
                    // logger.trace("reading columnName {}, columnValue {}",
                    // columnName , Util.debugBytesToHex(columnValue));
                    // If src (columnValue) is null, then a NullPointerException
                    // is thrown and the destination array is not modified.
                    if (columnValue == null) {
                        throw new NullPointerException("columnValue is null");
                    }
                    // int bytesToReadFromBlock = (int) Math.min(bytesToBeRead,
                    // (blockToBeRead.getDataLength() +
                    // blockToBeRead.getDataPosition()));
                    int bytesToReadFromBlock =
                            (int) Math.min(bytesToBeRead,
                                    blockToBeRead.getDataLength());
                    // logger.trace("bytesToBeRead {} blockToBeRead.getDataLength {}",
                    // bytesToBeRead, blockToBeRead.getDataLength());
                    // logger.trace("blockToBeRead.getDataPosition {}",
                    // blockToBeRead.getDataPosition());
                    int remain =
                            blockToBeRead.getDataLength()
                                    - blockToBeRead.getDataPosition();
                    if (resetPosition) {
                        blockToBeRead.setDataPosition(0);
                    }
                    if (!resetPosition && bytesToReadFromBlock > remain) {
                        if (remain > 0) {
                            // logger.trace("remain = {}", remain);
                            bytesToReadFromBlock = remain;

                        }
                    }
                    // logger.trace("blockToBeRead.getDataLength {} blockToBeRead.getDataPosition {}",
                    // blockToBeRead.getDataLength(),
                    // blockToBeRead.getDataPosition());
                    // logger.trace(String.format("off %s bytesToReadFromBlock %s ",
                    // off, bytesToReadFromBlock));
                    int srcPos = blockToBeRead.getDataPosition();
                    try {
                        if (resetPosition) {
                            srcPos = 0;
                        }
                        // logger.trace("reading current block {} of length {}",
                        // columnName, bytesToReadFromBlock);
                        // logger.trace(String.format("copying from columnValue of its length %s starting from srcPos %s with copy length %s => buffer array b.length %s off %s file %s",
                        // columnValue.length, srcPos, bytesToReadFromBlock,
                        // b.length, off, name));
                        System.arraycopy(columnValue, srcPos, b, off,
                                bytesToReadFromBlock);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        String whatHappened =
                                String.format(
                                        "columnValue length %s srcPos %s b length %s off %s bytesToBeRead %s",
                                        columnValue.length, srcPos, b.length,
                                        off, bytesToBeRead);
                        logger.error(whatHappened, e);
                        throw new IOException("unable to copy " + name);
                    }
                    // logger.trace("bytes read {}", Util.debugBytesToHex(b));
                    bytesToBeRead -= bytesToReadFromBlock;
                    totalRead += bytesToReadFromBlock;
                    off += bytesToReadFromBlock;
                    blockToBeRead.setDataPosition(blockToBeRead
                            .getDataPosition() + bytesToReadFromBlock);
                    // logger.trace(String.format("bytesToBeRead %s offset %s bytesToReadFromBlock %s blockToBeRead.getDataPosition() %s",
                    // bytesToBeRead, off, bytesToReadFromBlock,
                    // blockToBeRead.getDataPosition()));
                    resetPosition = true;
                }
            }
        }

        logger.trace("currentBlock {} and dataPosition {}",
                currentBlock.getBlockName(), currentBlock.getDataPosition());
        logger.trace(
                "currentBlock.getDataPosition() {} currentBlock.getDataLength() {}",
                currentBlock.getDataPosition(), currentBlock.getDataLength());

        if (currentBlock.getDataPosition() == currentBlock.getDataLength()) {
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock != null) {
                currentBlock = nextBlock;
                logger.trace("nextblock set " + currentBlock);
                logger.trace("nexblock {}", nextBlock.toString());
                currentBlock.setDataPosition(0);
            } else {
                currentBlock.setDataPosition(-1);
            }
            
            logger.trace("currentBlock {}", currentBlock.toString());
            logger.trace("using block {}", currentBlock.getBlockNumber());
        }

        if (totalRead != len) {
            logger.error("expected to read {} bytes but only read {} bytes",
                    len, totalRead);
        }

        return totalRead;
    }

    @Override
    public void close() {
        cassandraClient.close();
        cassandraClient = null;
    }

    public FileDescriptor getFD() {
        logger.trace("called getFD ");
        return fd;
    }

    /**
     * cannot change implementation of variable name to java.io.File as the code
     * is integrated into cassandra. Thus, we do alteration here to achieve the
     * behavior similar to java.io.File.getName() whilst still able to work with
     * cassandra.
     */
    public String getName() {
        // logger.trace("called getName {}", name);
        int index = name.lastIndexOf("/");
        index++;
        if (name.substring(index).equals("")) {
            return "/";
        } else {
            return name.substring(index);
        }
    }

    public void setName(String name) {
        logger.trace("called setName ");
        this.name = name;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public String getColumnFamily() {
        return this.columnFamily;
    }

    public int getBlockSize() {
        return this.blockSize;
    }

    public IOContext getMode() {
        return this.mode;
    }

    public CassandraClient getCassandraClient() {
        return cassandraClient;
    }

    @Override
    public Map<Counter, AtomicLong> getCounters() {
        Map<Counter, AtomicLong> counters =
                new EnumMap<Counter, AtomicLong>(
                        org.apache.monitor.Counter.class);
        counters.put(Counter.METRIC_MODE_MERGE, new AtomicLong(0));
        counters.put(Counter.METRIC_TOTAL_MODE_MERGE, new AtomicLong(0));
        return counters;
    }

    @Override
    public String getMonitorName() {
        return getClass().getName();
    }

    @Override
    public Path toPath() {
        Path result = filePath;
        if (result == null) {
            synchronized (this) {
                result = filePath;
                if (result == null) {
                    CassandraFileSystem cfs = new CassandraFileSystem(provider, cassandraDirectory);
                    result = cfs.getPath(getAbsolutePath());
                    filePath = result;
                }
            }
        }
        return result;
    }

    /**
     * This operation is expensive in cassandra point of view. As columns are
     * serialized on disk in the order denoted by the ordered_by attribute of a
     * ColumnFamily. Similarly, rows are serialized according to the
     * Partitioner. No choice, this is just design in cassandra. But in order to
     * implement it, we do the following
     * 
     * This operation is splitted to multiple operation, get all data, copy to
     * another new rowkey and delete the previous row.
     * 
     */
    @Override
    public boolean renameTo(File dest) {
        if (dest == null) {
            throw new NullPointerException();
        }
        if (fd == null) {
            return false;
        }
        if (this.isInvalid() || dest.isInvalid()) {
            return false;
        }
        if (dest.getName() == null || dest.getName().equals("")) {
            return false;
        }
        try {
            return columnOrientedFile.renameFile(this.getFileDescriptor(),
                    dest.getFileDescriptor());
        } catch (Exception e) {
            logger.error("unable to rename file " + e);
            return false;
        }
    }
    
    public long getTotalSpace() {
        logger.error("getTotalSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public long getUsableSpace() {
        logger.error("getUsableSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public long getFreeSpace() {
        logger.error("getFreeSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public File[] listFiles() {
        //logger.error("listFiles() IS CALLED!!! ");
        String[] files = list();
        List<File> fl = new ArrayList<File>();
        for (String file : files) {
            File f = new ACassandraFile("/", file, IOContext.READ, true, this.getKeyspace(), this.getColumnFamily(), this.getBlockSize());
            if (this.getParent(true).equals(f.getParent(true))) {
                fl.add(f);
            }
        }
        return fl.toArray(new ACassandraFile[fl.size()]);
    }
    
    public File[] listFiles(CassandraFileFilter filter) {
        String ss[] = list();
        if (ss == null) return null;
        
        logger.error("listFiles(FileFilter filter) IS CALLED!!! ");

        ArrayList<File> files = new ArrayList<>();
        for (String s : ss) {
            //File f = new ACassandraFile(this, s);
            File f = new ACassandraFile("/", this.getName(), this.getMode(), true, this.getKeyspace(), this.getColumnFamily(), this.getBlockSize());
            if ((filter == null) || filter.accept(f))
                files.add(f);
        }
        return files.toArray(new File[files.size()]);
    }
    
    public String getParent(boolean dummy) {
        int index = name.lastIndexOf("/");
        if (name.substring(0, index).equals("")) {
            return "/";
        } else {
            return name.substring(0, index);
        }
    }
    
    public File getParentFile() {
        String p = this.getParent(true);
        if (p == null) return null;
        return new ACassandraFile(p);
    }
    
    public boolean canRead() {
        return true;
    }
    
    public boolean isInvalid() {
        if (fd == null) {
            return true;
        }
        return false;
    }
    
    public FileDescriptor getFileDescriptor() {
        return this.fd;
    }
    
    /**
     * 
     */
    public void write(int b, boolean append)  throws IOException {

        String debug = String.format("writing b %s with appending %s", b, append);
        logger.trace(debug);

        BlockMap blocksToFlush = new BlockMap();
        
        int off = 0;
        byte[] src = {(byte)b};
        
        if (append) {
            currentBlock = fd.getLastBlock();
            
            if (currentBlock.getDataPosition() > 0) {
                logger.trace("creating prefragment");
                FileBlock preFragment = (FileBlock) currentBlock.clone();
                preFragment.setDataLength(currentBlock.getDataPosition());
                fd.insertBlock(currentBlock, preFragment, false);
            }

            int bytesLeftToWrite = src.length;
            int bytesAddedByWrite = 0;

            do {
                maybeRepositionCurrentBlock();
                // minimum of data to copy into cassandra.
                int dataLength = (int) Math.min(currentBlock.getBlockSize() - currentBlock.getPositionOffset(), bytesLeftToWrite);
                // current file block data length.
                int currentLength = currentBlock.getDataLength();
                FileBlock nextBlock;
                if (currentBlock.getDataPosition() == 0 && dataLength > currentBlock.getDataLength()) {
                    // no need create a new block
                    nextBlock = currentBlock;
                    nextBlock.setDataLength(dataLength);
                    debug = String.format("1 setting block %s for file %s with dataoffset %s",
                                    currentBlock.getBlockName(), name,
                                    currentBlock.getPositionOffset());
                    logger.trace(debug);
                } else {
                    // create a new block
                    nextBlock = fd.createBlock();
                    nextBlock.setDataLength(dataLength);
                    nextBlock.setDataOffset(currentBlock.getPositionOffset());
                    debug =
                            String.format(
                                    "1 setting block %s for file %s with dataoffset %s",
                                    currentBlock.getBlockName(), name,
                                    currentBlock.getPositionOffset());
                    logger.trace(debug);
                }

                byte[] partialBytes = new byte[dataLength];
                System.arraycopy(src, off, partialBytes, 0, dataLength);
                blocksToFlush.put(nextBlock.getBlockName(), partialBytes);
                logger.trace("added block {} with length {} to flush ",
                        nextBlock.getBlockName(), partialBytes.length);
                nextBlock.setDataPosition(dataLength);
                if (nextBlock != currentBlock) {
                    FileBlock blockToBeRemoved;
                    if (nextBlock.getDataPosition() > 0) {
                        blockToBeRemoved = currentBlock;
                        // add nextBlock into a position of the currentblock + 1.
                        // if currentblock position is 5, so nextblock position is 6
                        fd.insertBlock(currentBlock, nextBlock, true);
                    } else {
                        // add nextBlock into a position of the currentblock - 1.
                        // if currentblock position is 5, so nextblock position is 4
                        blockToBeRemoved = currentBlock;
                        fd.insertBlock(currentBlock, nextBlock, false);
                    }
                    for (; blockToBeRemoved != null
                            && blockToBeRemoved.getLastDataOffset() < nextBlock
                                    .getLastDataOffset(); blockToBeRemoved =
                            fd.getNextBlock(blockToBeRemoved)) {
                        fd.removeBlock(blockToBeRemoved);
                    }
                }
                bytesLeftToWrite -= dataLength;
                off += dataLength;
                if (fd.isLastBlock(nextBlock)) {
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
                debug =
                        String.format(
                                "2 setting block %s for file %s with dataoffset %s",
                                currentBlock.getBlockName(), name,
                                currentBlock.getPositionOffset());
                logger.trace(debug);
                postFragment
                        .setDataLength((int) (currentBlock.getDataLength() - postFragment
                                .getDataOffset()));

                fd.insertBlock(currentBlock, postFragment, true);
                currentBlock = postFragment;
                currentBlock.setBlockOffset(currentBlock.getBlockOffset()
                        + currentBlock.getDataPosition());
                currentBlock.setDataPosition(0);
            }

            maybeRepositionCurrentBlock();

            long now = new java.util.Date().getTime();
            if (bytesAddedByWrite > 0) {
                fd.setLength(fd.getLength() + bytesAddedByWrite);
            }
            fd.setLastAccessed(now);
            fd.setLastModified(now);
            logger.trace("file descriptor {}", FileDescriptorUtils.toString(fd));
            logger.trace("blocksToFlush size {}", blocksToFlush.size());
            if (blocksToFlush.size() > 1) {
                for (Entry<byte[], byte[]> entry : blocksToFlush.entrySet()) {
                    logger.trace("flushing block {} ", new String(entry.getKey()));
                }
            }
            columnOrientedFile.writeFileBlocks(fd, blocksToFlush);
            if (bytesLeftToWrite > 0) {
                logger.error("did not write fully as expected, remaining {}",
                        bytesLeftToWrite);
            }
            
        } else {
            this.columnOrientedFile.deleteFileBlocks(fd, false);
            this.fd = this.columnOrientedDirectory.resetFileDescriptor(name);

            currentBlock = fd.getFirstBlock();
            
            if (currentBlock.getDataPosition() > 0) {
                logger.trace("creating prefragment");
                FileBlock preFragment = (FileBlock) currentBlock.clone();
                preFragment.setDataLength(currentBlock.getDataPosition());
                fd.insertBlock(currentBlock, preFragment, false);
            }

            int bytesLeftToWrite = src.length;
            int bytesAddedByWrite = 0;
            
            do {
                maybeRepositionCurrentBlock();
                // minimum of data to copy into cassandra.
                int dataLength = (int) Math.min(currentBlock.getBlockSize() - currentBlock.getPositionOffset(), bytesLeftToWrite);
                // current file block data length.
                int currentLength = currentBlock.getDataLength();
                FileBlock nextBlock;
                if (currentBlock.getDataPosition() == 0 && dataLength > currentBlock.getDataLength()) {
                    // no need create a new block
                    nextBlock = currentBlock;
                    nextBlock.setDataLength(dataLength);
                    debug = String.format("1 setting block %s for file %s with dataoffset %s",
                                    currentBlock.getBlockName(), name,
                                    currentBlock.getPositionOffset());
                    logger.trace(debug);
                } else {
                    // create a new block
                    nextBlock = fd.createBlock();
                    nextBlock.setDataLength(dataLength);
                    nextBlock.setDataOffset(currentBlock.getPositionOffset());
                    debug =
                            String.format(
                                    "1 setting block %s for file %s with dataoffset %s",
                                    currentBlock.getBlockName(), name,
                                    currentBlock.getPositionOffset());
                    logger.trace(debug);
                }
                
                byte[] partialBytes = new byte[dataLength];
                System.arraycopy(src, off, partialBytes, 0, dataLength);
                blocksToFlush.put(nextBlock.getBlockName(), partialBytes);
                logger.trace("added block {} with length {} to flush ",
                        nextBlock.getBlockName(), partialBytes.length);
                nextBlock.setDataPosition(dataLength);
                if (nextBlock != currentBlock) {
                    FileBlock blockToBeRemoved;
                    if (nextBlock.getDataPosition() > 0) {
                        blockToBeRemoved = currentBlock;
                        // add nextBlock into a position of the currentblock + 1.
                        // if currentblock position is 5, so nextblock position is 6
                        fd.insertBlock(currentBlock, nextBlock, true);
                    } else {
                        // add nextBlock into a position of the currentblock - 1.
                        // if currentblock position is 5, so nextblock position is 4
                        blockToBeRemoved = currentBlock;
                        fd.insertBlock(currentBlock, nextBlock, false);
                    }
                    for (; blockToBeRemoved != null
                            && blockToBeRemoved.getLastDataOffset() < nextBlock
                                    .getLastDataOffset(); blockToBeRemoved =
                            fd.getNextBlock(blockToBeRemoved)) {
                        fd.removeBlock(blockToBeRemoved);
                    }
                }
                bytesLeftToWrite -= dataLength;
                off += dataLength;
                if (fd.isLastBlock(nextBlock)) {
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
                debug =
                        String.format(
                                "2 setting block %s for file %s with dataoffset %s",
                                currentBlock.getBlockName(), name,
                                currentBlock.getPositionOffset());
                logger.trace(debug);
                postFragment
                        .setDataLength((int) (currentBlock.getDataLength() - postFragment
                                .getDataOffset()));

                fd.insertBlock(currentBlock, postFragment, true);
                currentBlock = postFragment;
                currentBlock.setBlockOffset(currentBlock.getBlockOffset()
                        + currentBlock.getDataPosition());
                currentBlock.setDataPosition(0);
            }

            maybeRepositionCurrentBlock();

            long now = new java.util.Date().getTime();
            if (bytesAddedByWrite > 0) {
                fd.setLength(fd.getLength() + bytesAddedByWrite);
            }
            fd.setLastAccessed(now);
            fd.setLastModified(now);
            logger.trace("file descriptor {}", FileDescriptorUtils.toString(fd));
            logger.trace("blocksToFlush size {}", blocksToFlush.size());
            if (blocksToFlush.size() > 1) {
                for (Entry<byte[], byte[]> entry : blocksToFlush.entrySet()) {
                    logger.trace("flushing block {} ", new String(entry.getKey()));
                }
            }
            columnOrientedFile.writeFileBlocks(fd, blocksToFlush);
            if (bytesLeftToWrite > 0) {
                logger.error("did not write fully as expected, remaining {}",
                        bytesLeftToWrite);
            }
            
        }  
    }

    /**
     * {@link org.apache.lucene.cassandra.File#read() read()}
     * 
     * @throws IOException
     */
    @Override
    public int read() throws IOException {
        logger.trace("reading a byte");
        byte[] b = new byte[1];
        int value = read(b, 0, 1);
        if (value == -1) {
            return value;
        }
        return b[0];
    }

    @Override
    public FileSystem getFileSystem() {
        return fs;
    }

    @Override
    public boolean isAbsolute() {
     // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Path getRoot() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path getFileName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path getParent() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getNameCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Path getName(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean startsWith(Path other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean startsWith(String other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean endsWith(Path other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean endsWith(String other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Path normalize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolve(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolve(String other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolveSibling(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolveSibling(String other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path relativize(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public URI toUri() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path toAbsolutePath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public java.io.File toFile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events,
            Modifier... modifiers) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>... events)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<Path> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int compareTo(Path other) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object f) {
        return f instanceof File && ((File)f).getAbsolutePath().equals(name);
    }

}
