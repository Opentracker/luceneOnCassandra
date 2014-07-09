package org.apache.lucene.cassandra;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

// TODO
public class CassandraFileInputStream extends InputStream {
    
    /* File Descriptor - handle to the open file */
    //private final FileDescriptor fd;
    
    /* The path of the referenced file (null if the stream is created with a file descriptor) */
    private final String path;
    
    /**
     * Creates a <code>FileInputStream</code> by
     * opening a connection to an actual file,
     * the file named by the <code>File</code>
     * object <code>file</code> in the file system.
     * A new <code>FileDescriptor</code> object
     * is created to represent this file connection.
     * <p>
     * First, if there is a security manager,
     * its <code>checkRead</code> method  is called
     * with the path represented by the <code>file</code>
     * argument as its argument.
     * <p>
     * If the named file does not exist, is a directory rather than a regular
     * file, or for some other reason cannot be opened for reading then a
     * <code>FileNotFoundException</code> is thrown.
     *
     * @param      file   the file to be opened for reading.
     * @exception  FileNotFoundException  if the file does not exist,
     *                   is a directory rather than a regular file,
     *                   or for some other reason cannot be opened for
     *                   reading.
     * @exception  SecurityException      if a security manager exists and its
     *               <code>checkRead</code> method denies read access to the file.
     * @see        java.io.File#getPath()
     * @see        java.lang.SecurityManager#checkRead(java.lang.String)
     */
    public CassandraFileInputStream(File file) throws FileNotFoundException {
        String name = (file != null ? file.getPath() : null);
        SecurityManager security = System.getSecurityManager();
        if (security != null) {
            security.checkRead(name);
        }
        if (name == null) {
            throw new NullPointerException();
        }
        if (file.isInvalid()) {
            throw new FileNotFoundException("Invalid file path");
        }
        //fd = new FileDescriptor();
        //fd.incrementAndGetUseCount();
        this.path = name;
        open(name);
    }

    @Override
    public int read() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    /**
     * Opens the specified file for reading.
     * @param name the name of the file
     */
    private native void open(String name) throws FileNotFoundException;
    
    /**
     * Closes this file input stream and releases any system resources
     * associated with the stream.
     *
     * <p> If this stream has an associated channel then the channel is closed
     * as well.
     *
     * @exception  IOException  if an I/O error occurs.
     *
     */
    public void close() throws IOException {
        //synchronized (closeLock) {
        //    if (closed) {
        //        return;
        //    }
        //    closed = true;
        //}
        //if (channel != null) {
            /*
             * Decrement the FD use count associated with the channel
             * The use count is incremented whenever a new channel
             * is obtained from this stream.
             */
           //fd.decrementAndGetUseCount();
         //  channel.close();
        //}

        /*
         * Decrement the FD use count associated with this stream
         */
        //int useCount = fd.decrementAndGetUseCount();

        /*
         * If FileDescriptor is still in use by another stream, the finalizer
         * will not close it.
         */
        //if ((useCount <= 0) || !isRunningFinalize()) {
         //   close0();
        //}
    }

}
