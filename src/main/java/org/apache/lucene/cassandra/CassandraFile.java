package org.apache.lucene.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.IOContext;
import org.apache.monitor.Counter;
import org.apache.monitor.JmxMonitor;
import org.apache.monitor.MonitorType;
import org.apache.monitor.OpentrackerClientMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraFile implements Closeable, MonitorType {

    private String name = null;
    private String keyspace = null;
    private String columnFamily = null;
    private String cassandraDirectory = null;
    private IOContext mode = null;
    private long length;
    //private boolean isDelete = false;
    private long lastModified = -1;
    private int blockSize;
    private FileBlock currentBlock = null;
    
    private static Logger logger = LoggerFactory.getLogger(CassandraFile.class);
    private CassandraClient cassandraClient = null;
    private ColumnOrientedDirectory columnOrientedDirectory;
    private FileDescriptor fd = null;
    private ColumnOrientedFile columnOrientedFile = null;
    private boolean isModeMerge = false;
    private OpentrackerClientMonitor monitor;
    
    public CassandraFile(String directory, String name, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize) {
        logger.trace("CassandraFile called 3 {} mode {}", name, mode);
        if (directory == null) {
            this.name = name;
        } else {
            this.name = directory + name;
        }
        this.mode = mode;
        this.blockSize = blockSize;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.cassandraDirectory = directory;
        boolean readOnly = true;
        monitor = JmxMonitor.getInstance().getCassandraMonitor(this);
        try {
            cassandraClient = new CassandraClient("localhost", 9160, frameMode, keyspace, columnFamily, blockSize);
            this.columnOrientedDirectory = new ColumnOrientedDirectory(cassandraClient, blockSize);
            this.columnOrientedFile = new ColumnOrientedFile(cassandraClient);
            if (mode == null || mode.context == IOContext.Context.DEFAULT || mode.context == IOContext.Context.FLUSH 
                    || mode.context == IOContext.Context.MERGE 
                    || (mode.context == IOContext.Context.READ && name.equals("segments.gen"))) {
                // when mode == null when writing write.lock
                this.fd = this.columnOrientedDirectory.getFileDescriptor(this.name, true);
                readOnly = false;
            } else if (mode.context == IOContext.Context.READ) {
                this.fd = this.columnOrientedDirectory.getFileDescriptor(this.name);
                readOnly = true;
            }
            if (fd == null) {
                if (!readOnly) {
                    throw new IOException("fd is null, unable to retrieve file " + this.name);
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
    }

    public CassandraFile getFile() {
        logger.trace("CassandraFile getFile ");
        return this;
    }

    /**
     * check if file exists in cassandra by checking if row exists.
     *  
     * 
     * @return if file descriptor exists.
     */
    public boolean exists() {
        
        FileDescriptor descriptor = null;
        boolean isExists = false;
        try {
            descriptor = columnOrientedDirectory.getFileDescriptor(name);
        } catch (IOException e) {
            isExists = false;
        }
        if (descriptor != null) {
            isExists = true;
        }
        logger.trace("called exists {} for file {}", isExists, name);
        return isExists;
    }

    public boolean isDirectory() {
        logger.trace("called isDirectory always returning true");
        return true;
    }

    public String[] list(java.io.FilenameFilter filenameFilter) {
        String[] files = {};
        try {
            files = columnOrientedDirectory.getFileNames();
        } catch (IOException e) {
            logger.error("unable to list ", e);
        }
        return files;
    }
    
    public String[] list() {
        String[] files = {};
        try {
            files = columnOrientedDirectory.getFileNames();
        } catch (IOException e) {
            logger.error("unable to list ", e);
        }
        return files;
    }

    public String getCanonicalPath() throws IOException {
        if (cassandraDirectory == null) {
            logger.trace("called getCanonicalPath returning / for file {}", name);
            return new String("/");
        }
        logger.trace("called getCanonicalPath returning {} for file {}", cassandraDirectory, name);
        return cassandraDirectory;
    }

    public long length() {
        return length;
    }

    // delete this file in cassandra and return true if it deleted , any thing else, return false;
    public boolean delete() {
        logger.trace("deleting file {}", name);
        if (fd != null) {
            try {
                fd.setDeleted(true);
                columnOrientedDirectory.setFileDescriptor(fd);
                cassandraClient.setColumns(ByteBufferUtil.bytes(fd.getName()), null);
            } catch (IOException e) {
                logger.error("unable to delete file " + name, e);
                return false;
            }
        }
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

    public boolean createNewFile() throws IOException {
        logger.trace("creating {}", name);
        try {
            FileDescriptor fd = columnOrientedDirectory.getFileDescriptor(name, true);
            fd.setLastModified(System.currentTimeMillis());
            columnOrientedDirectory.setFileDescriptor(fd);
        } catch (Exception e) {
            logger.error("unable to create a new file " + name, e);
            throw new IOException("unable to create a new file " + name);
        }
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
     * in python talk it would be something like
     * file.write(b[off:off+len]))
     * so the byte array , b and take the data from specified offset and until offset + length.
     * then write this length of data into the file
     * 
     * Writes <code>len</code> bytes from the specified byte array 
     * starting at offset <code>off</code> to this file. overwrite.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("array b is null");
        }
        
        if (off < 0 ) {
            throw new IndexOutOfBoundsException("offset for the file which is going to be written must not be negative");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException("byte array length must not be negative");
        }
        if (len > b.length - off) {
            throw new IndexOutOfBoundsException("write length must not greater than buffer length minus buffer offset");
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

        String debug = String.format("flushing buffer.. bytes %s offset %s length %s", Util.debugBytesToHex(b), off, len);            
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
            int dataLength = (int) Math.min(currentBlock.getBlockSize() - currentBlock.getPositionOffset(), bytesLeftToWrite);
            // current file block data length.
            int currentLength = currentBlock.getDataLength();
            FileBlock nextBlock;
            if (currentBlock.getDataPosition() == 0 && dataLength > currentBlock.getDataLength()) {
                // no need create a new block
                nextBlock = currentBlock;
                nextBlock.setDataLength(dataLength);
                debug = String.format("1 setting block %s for file %s with dataoffset %s", currentBlock.getBlockName(), name, currentBlock.getPositionOffset());
                logger.trace(debug);
            } else {
                // create a new block
                nextBlock = fd.createBlock();
                nextBlock.setDataLength(dataLength);
                nextBlock.setDataOffset(currentBlock.getPositionOffset());
                debug = String.format("1 setting block %s for file %s with dataoffset %s", currentBlock.getBlockName(), name, currentBlock.getPositionOffset());
                logger.trace(debug);
            }
            byte[] partialBytes = new byte[dataLength];
            System.arraycopy(b, off, partialBytes, 0, dataLength);
            blocksToFlush.put(nextBlock.getBlockName(), partialBytes);
            logger.trace("added block {} with length {} to flush ", nextBlock.getBlockName(), partialBytes.length);
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
            debug = String.format("2 setting block %s for file %s with dataoffset %s", currentBlock.getBlockName(), name, currentBlock.getPositionOffset());
            logger.trace(debug);
            postFragment.setDataLength((int) (currentBlock.getDataLength() - postFragment.getDataOffset()));

            fd.insertBlock(currentBlock, postFragment, true);
            currentBlock = postFragment;
            currentBlock.setBlockOffset(currentBlock.getBlockOffset() + currentBlock.getDataPosition());
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
            for (Entry<byte[], byte[]> entry  : blocksToFlush.entrySet()) {
                logger.trace("flushing block {} ", new String(entry.getKey()));
            }
        }
        columnOrientedFile.writeFileBlocks(fd, blocksToFlush);
        if (bytesLeftToWrite > 0) {
            logger.error("did not write fully as expected, remaining {}", bytesLeftToWrite);
        }
    }
    
    /**
     * In the event the file pointer is currently positioned at the exact
     * end of the data range in the current block, then reposition to the
     * first byte in the ensuing block. Furthermore, if there is no ensuing
     * block, then create a brand-new block, append it to the end of the
     * file, and move into that new block.
     */
    private void maybeRepositionCurrentBlock() {
        logger.trace("maybeRepositionCurrentBlock");
        // this if means use up all the blocksize allocated.
        logger.trace("dataPos {} dataLength {}", currentBlock.getDataPosition(),  currentBlock.getDataLength());
        logger.trace("positionOffset {} blockSize {}", currentBlock.getPositionOffset(), currentBlock.getBlockSize());
        if (currentBlock.getDataPosition() == currentBlock.getDataLength() && currentBlock.getPositionOffset() == currentBlock.getBlockSize()) {
            // example if current block is BLOCK-2, nextblock is BLOCK-3
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock == null) {
                // do not have next block to the current block, which mean the current lbock is the last block,
                // and then create a new one.
                nextBlock = fd.createBlock();
                logger.trace("currentblock number {} new nextblock number {}", currentBlock.getBlockNumber(), nextBlock.getBlockNumber());
                fd.insertBlock(currentBlock, nextBlock, true);
            }
            // switch currentblock into new nextblock.
            currentBlock = nextBlock;
        }
    }

    /**
     * Sets the file-pointer offset, measured from the beginning of this 
     * file, at which the next read or write occurs.  The offset may be 
     * set beyond the end of the file. Setting the offset beyond the end 
     * of the file does not change the file length.  The file length will 
     * change only by writing after the offset has been set beyond the end 
     * of the file. 
     *
     * @param      pos   the offset position, measured in bytes from the 
     *                   beginning of the file, at which to set the file 
     *                   pointer.
     * @exception  IOException  if <code>pos</code> is less than 
     *                          <code>0</code> or if an I/O error occurs.
     */
    public void seek(long pos) throws IOException {
        if (pos < 0) {
            throw new IOException("position cannot be negative");
        }
        if (pos > 2147483647) {
            logger.error("seek {} out of the bound 2147483647", pos);
        }
        //logger.trace("called seek {}", pos);
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
        logger.trace("last data offset is now {}", currentBlock.getLastDataOffset());
        logger.trace("positionOffset is now {}", currentBlock.getPositionOffset());
    }

    /**
     * Sets the length of this file.
     *
     * <p> If the present length of the file as returned by the
     * <code>length</code> method is greater than the <code>newLength</code>
     * argument then the file will be truncated.  In this case, if the file
     * offset as returned by the <code>getFilePointer</code> method is greater
     * than <code>newLength</code> then after this method returns the offset
     * will be equal to <code>newLength</code>.
     *
     * <p> If the present length of the file as returned by the
     * <code>length</code> method is smaller than the <code>newLength</code>
     * argument then the file will be extended.  In this case, the contents of
     * the extended portion of the file are not defined.
     *
     * @param      newLength    The desired length of the file
     * @exception  IOException  If an I/O error occurs
     * @since      1.2
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
     * Reads up to <code>len</code> bytes of data from this file into an 
     * array of bytes. This method blocks until at least one byte of input 
     * is available. 
     * <p>
     * Although <code>RandomAccessFile</code> is not a subclass of 
     * <code>InputStream</code>, this method behaves in exactly the 
     * same way as the {@link InputStream#read(byte[], int, int)} method of 
     * <code>InputStream</code>.
     *
     * @param      b     the buffer into which the data is read.
     * @param      off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param      len   the maximum number of bytes read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception  IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or if
     * some other I/O error occurs.
     * @exception  NullPointerException If <code>b</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative, 
     * <code>len</code> is negative, or <code>len</code> is greater than 
     * <code>b.length - off</code>
     */
    public int read(byte[] b, int off, int len) throws IOException {
        String debug = String.format("buffer '%s' offset %s len %s", Util.debugBytesToHex(b), off, len);
        logger.trace(debug);
        
        if (b == null) {
            throw new NullPointerException("array b is null");
        }
        
        if (off < 0 ) {
            throw new IndexOutOfBoundsException("buffer offset must not be negative");
        }
        if (len < 0) {
            throw new IndexOutOfBoundsException("read length must not be negative");
        }
        if (len > b.length - off) {
            throw new IndexOutOfBoundsException("read length must not greater than buffer length minus buffer offset");
        }
        
        if (len == 0) {
            return 0;
        }
        
        // contain unique block name to be read.
        // block-0, block-1 ... block-N
        Set<byte[]> blockNames = new TreeSet<byte[]>(BlockMap.BYTE_ARRAY_COMPARATOR);
        List<FileBlock> blocksToBeRead = new ArrayList<FileBlock>();

        int bytesToBeRead = len;

        //logger.trace("fileDescriptor name = '{}' current file length = '{}'", fd.getName(), length);
        //logger.trace("fileDescriptor length {} fileDescriptor total blocks {}", fd.getLength(), fd.getBlocks().size());
        
        boolean initialStartBlock = true;

        do {
            byte[] columnName = currentBlock.getBlockName().getBytes();
            if (!blockNames.contains(columnName)) {
                logger.trace("adding columnName {} dataPosition {}", new String(columnName), currentBlock.getDataPosition());
                blockNames.add(columnName);
            }
            blocksToBeRead.add(currentBlock);
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock == null) {
                //logger.trace("next block is null, breaking out of loop");
                break;
            }
            if (currentBlock.getDataPosition() >= currentBlock.getDataLength()) {
                logger.warn("dataPosition {} greater than or equal to dataLength {}", currentBlock.getDataPosition(), currentBlock.getDataLength());
            }
            if (initialStartBlock) {
                bytesToBeRead -= (currentBlock.getDataLength() - currentBlock.getDataPosition());
            } else {
                bytesToBeRead -= currentBlock.getDataLength();
            }
            if (bytesToBeRead < 0) {
                logger.trace("bytesToBeRead {} currentBlock name {}", bytesToBeRead, currentBlock.getBlockName());           
            } else {
                currentBlock = nextBlock;
            }
            initialStartBlock = false;
        } while (bytesToBeRead > 0);

        //logger.trace("blockNames size {} blocksToBeRead size {}", blockNames.size(), blocksToBeRead.size());
        
        BlockMap blockMap = null;

        try {
            // get the row specified by the fd and read all values inside the column specified by column name blockNames.
            blockMap = columnOrientedFile.readFileBlocks(fd, blockNames);
        } catch (Exception e) {
            throw new IOException("cannot read column from cassandra.");
        }
        
        bytesToBeRead = len;
        int totalRead = 0;
        boolean resetPosition = false;
        for (FileBlock blockToBeRead : blocksToBeRead) {
            //logger.trace("reading fileblock {} of its length {}", blockToBeRead.getBlockName(), blockToBeRead.getDataLength());
            for (Map.Entry<byte[], byte[]> columnEntry : blockMap.entrySet()) {
                String columnName = new String(columnEntry.getKey());
                byte[] columnValue = columnEntry.getValue();
                if (columnName.equals(blockToBeRead.getBlockName())) {
                    //logger.trace("reading columnName {}, columnValue {}", columnName , Util.debugBytesToHex(columnValue));
                    // If src (columnValue) is null, then a NullPointerException is thrown and the destination array is not modified.
                    if (columnValue == null) {
                        throw new NullPointerException("columnValue is null");
                    }
                    //int bytesToReadFromBlock = (int) Math.min(bytesToBeRead, (blockToBeRead.getDataLength() + blockToBeRead.getDataPosition()));
                    int bytesToReadFromBlock = (int) Math.min(bytesToBeRead, blockToBeRead.getDataLength());
                    //logger.trace("bytesToBeRead {} blockToBeRead.getDataLength {}", bytesToBeRead, blockToBeRead.getDataLength());
                    //logger.trace("blockToBeRead.getDataPosition {}", blockToBeRead.getDataPosition());
                    int remain = blockToBeRead.getDataLength() - blockToBeRead.getDataPosition();
                    if (resetPosition) {
                        blockToBeRead.setDataPosition(0);
                    }
                    if (!resetPosition && bytesToReadFromBlock > remain) {
                        if (remain > 0) {
                            //logger.trace("remain = {}", blockToBeRead.getDataLength() - blockToBeRead.getDataPosition());
                            bytesToReadFromBlock = remain;
                            
                        }
                    }
                    //logger.trace("blockToBeRead.getDataLength {} blockToBeRead.getDataPosition {}", blockToBeRead.getDataLength(), blockToBeRead.getDataPosition());
                    //logger.trace(String.format("off %s bytesToReadFromBlock %s ", off, bytesToReadFromBlock));
                    int srcPos = blockToBeRead.getDataPosition();
                    try {
                        if (resetPosition) {
                            srcPos = 0;
                        }
                        //logger.trace("reading current block {} of length {}", columnName, bytesToReadFromBlock);
                        //logger.trace(String.format("copying from columnValue of its length %s starting from srcPos %s with copy length %s => buffer array b.length %s off %s file %s", columnValue.length, srcPos, bytesToReadFromBlock, b.length, off, name));
                        System.arraycopy(columnValue, srcPos, b, off, bytesToReadFromBlock);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        String whatHappened = String.format("columnValue length %s srcPos %s b length %s off %s bytesToBeRead %s", columnValue.length, srcPos, b.length, off, bytesToBeRead);
                        logger.error(whatHappened, e);
                        throw new IOException("unable to copy " + name);
                    }
                    //logger.trace("bytes read {}", Util.debugBytesToHex(b));
                    bytesToBeRead -= bytesToReadFromBlock;
                    totalRead += bytesToReadFromBlock;
                    off += bytesToReadFromBlock;
                    blockToBeRead.setDataPosition(blockToBeRead.getDataPosition() + bytesToReadFromBlock);
                    //logger.trace(String.format("bytesToBeRead %s offset %s bytesToReadFromBlock %s blockToBeRead.getDataPosition() %s", bytesToBeRead, off, bytesToReadFromBlock, blockToBeRead.getDataPosition()));
                    resetPosition = true;
                }
            }
        }

        logger.trace("currentBlock {} and dataPosition {}", currentBlock.getBlockName(), currentBlock.getDataPosition());
        logger.trace("currentBlock.getDataPosition() {} currentBlock.getDataLength() {}", currentBlock.getDataPosition(), currentBlock.getDataLength());

        if (currentBlock.getDataPosition() == currentBlock.getDataLength()) {
            FileBlock nextBlock = fd.getNextBlock(currentBlock);
            if (nextBlock != null) {
                currentBlock = nextBlock;
                logger.trace("nextblock set " + currentBlock);
                logger.trace("nexblock {}", nextBlock.toString());
            }
            currentBlock.setDataPosition(0);
            logger.trace("currentBlock {}", currentBlock.toString());
            logger.trace("using block {}", currentBlock.getBlockNumber());
        }
                
        if (totalRead != len) {
            logger.error("expected to read {} bytes but only read {} bytes", len, totalRead);
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

    public String getName() {
        //logger.trace("called getName {}", name);
        return name;
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
        Map<Counter, AtomicLong> counters = new EnumMap<Counter, AtomicLong>(org.apache.monitor.Counter.class);
        counters.put(Counter.METRIC_MODE_MERGE, new AtomicLong(0));
        counters.put(Counter.METRIC_TOTAL_MODE_MERGE, new AtomicLong(0));
        return counters;
    }

    @Override
    public String getMonitorName() {
        return getClass().getName();
    }    

}
