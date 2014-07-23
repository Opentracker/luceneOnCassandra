package org.apache.lucene.cassandra;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import org.apache.lucene.cassandra.nio.FileChannelImpl;

import sun.misc.IoTrace;

public class CassandraFileInputStream extends InputStream {

    /* File Descriptor - handle to the open file */
    private final FileDescriptor fd;

    /*
     * The path of the referenced file (null if the stream is created with a
     * file descriptor)
     */
    private final String path;

    private FileChannel channel = null;

    private final Object closeLock = new Object();

    private volatile boolean closed = false;

    private final File file;

    /**
     * Creates a <code>FileInputStream</code> by opening a connection to an
     * actual file, the file named by the <code>File</code> object
     * <code>file</code> in the file system. A new <code>FileDescriptor</code>
     * object is created to represent this file connection.
     * <p>
     * First, if there is a security manager, its <code>checkRead</code> method
     * is called with the path represented by the <code>file</code> argument as
     * its argument.
     * <p>
     * If the named file does not exist, is a directory rather than a regular
     * file, or for some other reason cannot be opened for reading then a
     * <code>FileNotFoundException</code> is thrown.
     * 
     * @param file
     *            the file to be opened for reading.
     * @exception FileNotFoundException
     *                if the file does not exist, is a directory rather than a
     *                regular file, or for some other reason cannot be opened
     *                for reading.
     * @exception SecurityException
     *                if a security manager exists and its
     *                <code>checkRead</code> method denies read access to the
     *                file.
     * @see java.io.File#getPath()
     * @see java.lang.SecurityManager#checkRead(java.lang.String)
     */
    public CassandraFileInputStream(File file) throws FileNotFoundException {
        String name = (file != null ? file.getPath() : null);
        // SecurityManager security = System.getSecurityManager();
        // if (security != null) {
        // security.checkRead(name);
        // }
        if (name == null) {
            throw new NullPointerException();
        }
        if (file.isInvalid()) {
            throw new FileNotFoundException("Invalid file path");
        }
        this.file = file;
        // fd = new FileDescriptor();
        fd = file.getFileDescriptor();
        // fd.incrementAndGetUseCount();
        this.path = name;
        // open(name);
    }

    /**
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the stream
     * has been reached, the value <code>-1</code> is returned. This method
     * blocks until input data is available, the end of the stream is detected,
     * or an exception is thrown.
     * 
     * @return the next byte of data, or <code>-1</code> if the end of the file
     *         is reached.
     * @exception IOException
     *                if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
        // http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/misc/IoTrace.java#IoTrace
        Object traceContext = IoTrace.fileReadBegin(path);
        int b = 0;
        try {
            // b = read0();
            b = file.read();
        } finally {
            // http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/misc/IoTrace.java#IoTrace
            IoTrace.fileReadEnd(traceContext, b == -1 ? 0 : 1);
        }
        return b;
    }

    //public native int read0();

    public FileChannel getChannel() {
        synchronized (this) {
            if (channel == null) {
                channel =
                        FileChannelImpl
                                .open(fd, path, false, true, false, this);

                /*
                 * Increment fd's use count. Invoking the channel's close()
                 * method will result in decrementing the use count set for the
                 * channel.
                 */
                // fd.incrementAndGetUseCount();
            }
            return channel;
        }
    }

    /**
     * Opens the specified file for reading.
     * 
     * @param name
     *            the name of the file
     */
    // private native void open(String name) throws FileNotFoundException;

    /**
     * Closes this file input stream and releases any system resources
     * associated with the stream.
     * 
     * <p>
     * If this stream has an associated channel then the channel is closed as
     * well.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     * 
     */
    public void close() throws IOException {
        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (channel != null) {
            /*
             * Decrement the FD use count associated with the channel The use
             * count is incremented whenever a new channel is obtained from this
             * stream.
             */
            // fd.decrementAndGetUseCount();
            channel.close();
        }

        /*
         * Decrement the FD use count associated with this stream
         */
        // int useCount = fd.decrementAndGetUseCount();

        /*
         * If FileDescriptor is still in use by another stream, the finalizer
         * will not close it.
         */
        // if ((useCount <= 0) || !isRunningFinalize()) {
        // close0();
        // }
    }

}
