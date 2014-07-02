package org.apache.lucene.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;

import org.apache.lucene.cassandra.nio.FileChannelImpl;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ACassandraRandomAccessFile implements RandomAccessFile,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    ACassandraFile file;

    private FileChannel channel = null;
    private IOContext mode = null;

    private static Logger logger = LoggerFactory
            .getLogger(ACassandraRandomAccessFile.class);

    Object lock = new Object();

    /**
     * 
     * @param path
     * @param mode
     *            rw
     */
    public ACassandraRandomAccessFile(File path, IOContext mode,
            boolean frameMode, String keyspace, String columnFamily,
            int bufferSize) {
        // logger.trace("called CassandraRandomAccessFile {} {}",
        // path.getName(), mode);
        // file = new CassandraFile(path.getFile(), path.getName(), mode);
        file =
                new ACassandraFile(Util.getCassandraPath(path),
                        Util.getFileName(path), mode, frameMode, keyspace,
                        columnFamily, bufferSize);
    }

    public ACassandraRandomAccessFile(File path, String mode) {
        IOContext context = IOContext.DEFAULT;
        if (mode.equals("r")) {
            context = IOContext.READ;
        }
        file =
                new ACassandraFile(Util.getCassandraPath(path),
                        Util.getFileName(path), context, true, "lucene0",
                        "index0", 16384);
    }

    public long length() throws IOException {
        logger.trace("called length {} of this file {}", file.length(),
                this.file.getName());
        return file.length();
    }

    public void close() throws IOException {
        logger.trace("called close");
        file.close();
    }

    // Writes n bytes from the specified byte array starting at offset to this
    // file.
    public static int writeCount = 0;
    public static long writeTime = 0;
    public void write(byte[] b, int offset, int n) throws IOException {
       synchronized (lock) {
            long ms = System.currentTimeMillis();
            writeCount++;
            logger.trace("called write");
            file.write(b, offset, n);
            writeTime += System.currentTimeMillis() - ms;
        }
    }

    /*
     * Sets the file-pointer offset, measured from the beginning of this file,
     * at which the next read or write occurs. The offset may be set beyond the
     * end of the file. Setting the offset beyond the end of the file does not
     * change the file length. The file length will change only by writing after
     * the offset has been set beyond the end of the file.
     */
    public static int seekCount = 0;
    public static long seekTime = 0;
    public void seek(long pos) throws IOException {
        synchronized (lock) {
            long ms = System.currentTimeMillis();
            seekCount++;
            logger.trace("called seek {}", pos);
            file.seek(pos);
            seekTime += System.currentTimeMillis() - ms;
        }
    }

    // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#setLength%28long%29
    public void setLength(long length) {
        logger.trace("called setLength");
        file.setLength(length);
    }

    // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#read%28byte[],%20int,%20int%29
    public static int readCount = 0;
    public static long readTime = 0;
    public int read(byte[] b, int off, int len) throws IOException {
        String debug =
                String.format(
                        "reading a length of %s from this file %s into byte array  %s with starting offset %s",
                        len, file.getName(), Util.debugBytesToHex(b), off);
        // logger.trace(debug);
        synchronized (lock) {
            long ms = System.currentTimeMillis();
            readCount++;
            int read = file.read(b, off, len);
            logger.info("read {} off {} ", read, off);
            logger.info("Util.bytesToHex({})", Util.bytesToHex(b));
            readTime += System.currentTimeMillis() - ms;
            return read;
        }
    }

    // don't actually need this method.
    public Closeable getFile() {
        logger.trace("called getFile");
        return file;
    }

    /**
     * Force all system buffers to synchronize with the underlying device. This
     * method returns after all modified data and attributes of this
     * FileDescriptor have been written to the relevant device(s). In
     * particular, if this FileDescriptor refers to a physical storage medium,
     * such as a file in a file system, sync will not return until all in-memory
     * modified copies of buffers associated with this FileDescriptor have been
     * written to the physical medium.
     * 
     * sync is meant to be used by code that requires physical storage (such as
     * a file) to be in a known state For example, a class that provided a
     * simple transaction facility might use sync to ensure that all changes to
     * a file caused by a given transaction were recorded on a storage medium.
     * 
     * sync only affects buffers downstream of this FileDescriptor. If any
     * in-memory buffering is being done by the application (for example, by a
     * BufferedOutputStream object), those buffers must be flushed into the
     * FileDescriptor (for example, by invoking OutputStream.flush) before that
     * data will be affected by sync.
     * 
     * @exception SyncFailedException
     *                Thrown when the buffers cannot be flushed, or because the
     *                system cannot guarantee that all the buffers have been
     *                synchronized with physical media.
     * @since JDK1.1
     */
    public static int syncCount = 0;
    public static long syncTime = 0;

    public void getFDsync() throws IOException {
        logger.trace("called getFDsync");
        synchronized (lock) {
            syncCount++;
            long ms = System.currentTimeMillis();

            FileDescriptor fd = file.getFD();

            String existingFD = FileDescriptorUtils.toString(fd);

            ColumnOrientedFile cof =
                    new ColumnOrientedFile(file.getCassandraClient());
            FileDescriptor cassandraFD =
                    cof.getFileDescriptor(file.getName(), file.getBlockSize());

            String cassandraFDString =
                    FileDescriptorUtils.toString(cassandraFD);

            if (!cassandraFDString.equals(existingFD)) {
                logger.error("existing fd {}", FileDescriptorUtils.toString(fd));
                logger.error("cassandra fd {}",
                        FileDescriptorUtils.toString(cassandraFD));
            }

            cof.setFileDescriptor(file.getName(), fd);
            syncTime += System.currentTimeMillis() - ms;

        }
    }

    /**
     * Tests if this file descriptor object is valid.
     * 
     * @return <code>true</code> if the file descriptor object represents a
     *         valid, open file, socket, or other active I/O connection;
     *         <code>false</code> otherwise.
     */
    public boolean getFDvalid() {
        logger.trace("called getFDvalid ");

        if (file.getFD() != null) {
            return true;
        }

        return false;
    }

    public FileChannel getChannel() {
        synchronized (this) {
            if (channel == null) {
                boolean rw = false;
                if (mode.context == IOContext.Context.READ) {
                    rw = false;
                } else if (mode.context == IOContext.Context.DEFAULT) {
                    rw = true;
                }
                channel = FileChannelImpl.open(file.getFD(), true, rw, this);
            }
            return channel;
        }
    }

}
