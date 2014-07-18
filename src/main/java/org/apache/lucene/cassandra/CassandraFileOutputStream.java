package org.apache.lucene.cassandra;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;

import org.apache.lucene.cassandra.nio.FileChannelImpl;

import sun.misc.IoTrace;

/**
 * A file output stream is an output stream for writing data to a
 * <code>File</code> or to a <code>FileDescriptor</code>. Whether or not
 * a file is available or may be created depends upon the underlying
 * platform.  Some platforms, in particular, allow a file to be opened
 * for writing by only one <tt>CassandraFileOutputStream</tt> (or other
 * file-writing object) at a time.  In such situations the constructors in
 * this class will fail if the file involved is already open.
 *
 * <p><code>CassandraFileOutputStream</code> is meant for writing streams 
 * of raw bytes such as image data. For writing streams of characters, consider 
 * using <code>FileWriter</code>.
 * 
 */
public class CassandraFileOutputStream extends OutputStream {
    
    /**
     * The system dependent file descriptor.
     */
    private final FileDescriptor fd;
    
    /**
     * True if the file is opened for append.
     */
    private final boolean append;
    
    /**
     * The path of the referenced file (null if the stream is created with a file descriptor)
     */
    private final String path;
    
    private final Object closeLock = new Object();
    private volatile boolean closed = false;
    
    private final File file;
    
    
    /**
     * Creates a file output stream to write to the file represented by
     * the specified <code>File</code> object. A new
     * <code>FileDescriptor</code> object is created to represent this
     * file connection.
     * <p>
     * First, if there is a security manager, its <code>checkWrite</code>
     * method is called with the path represented by the <code>file</code>
     * argument as its argument.
     * <p>
     * If the file exists but is a directory rather than a regular file, does
     * not exist but cannot be created, or cannot be opened for any other
     * reason then a <code>FileNotFoundException</code> is thrown.
     *
     * @param      file               the file to be opened for writing.
     * @exception  FileNotFoundException  if the file exists but is a directory
     *                   rather than a regular file, does not exist but cannot
     *                   be created, or cannot be opened for any other reason
     * @exception  SecurityException  if a security manager exists and its
     *               <code>checkWrite</code> method denies write access
     *               to the file.

     * @see        java.lang.SecurityException
     * @see        java.lang.SecurityManager#checkWrite(java.lang.String)
     */
    public CassandraFileOutputStream(File file) throws FileNotFoundException {
        this(file, false);
    }
    
    /**
     * Creates a file output stream to write to the file represented by
     * the specified <code>File</code> object. If the second argument is
     * <code>true</code>, then bytes will be written to the end of the file
     * rather than the beginning. A new <code>FileDescriptor</code> object is
     * created to represent this file connection.
     * <p>
     * First, if there is a security manager, its <code>checkWrite</code>
     * method is called with the path represented by the <code>file</code>
     * argument as its argument.
     * <p>
     * If the file exists but is a directory rather than a regular file, does
     * not exist but cannot be created, or cannot be opened for any other
     * reason then a <code>FileNotFoundException</code> is thrown.
     *
     * @param      file               the file to be opened for writing.
     * @param     append      if <code>true</code>, then bytes will be written
     *                   to the end of the file rather than the beginning
     * @exception  FileNotFoundException  if the file exists but is a directory
     *                   rather than a regular file, does not exist but cannot
     *                   be created, or cannot be opened for any other reason
     * @exception  SecurityException  if a security manager exists and its
     *               <code>checkWrite</code> method denies write access
     *               to the file.
     * @see        java.lang.SecurityException
     * @see        java.lang.SecurityManager#checkWrite(java.lang.String)
     */
    public CassandraFileOutputStream(File file, boolean append)
        throws FileNotFoundException
    {
        String name = (file != null ? file.getPath() : null);
        //SecurityManager security = System.getSecurityManager();
        //if (security != null) {
            //security.checkWrite(name);
        //}
        if (name == null) {
            throw new NullPointerException();
        }
        if (file.isInvalid()) {
            throw new FileNotFoundException("Invalid file path");
        }
        //this.fd = new FileDescriptor();
        this.file = file;
        this.append = append;
        if (this.append) {
            System.out.println("existing fd " + this.append);
            this.fd = file.getFileDescriptor();
        } else {
            System.out.println("new fd " + this.append);
            this.fd = new FileDescriptor(name, 16384);
        }
        this.path = name;
        //fd.incrementAndGetUseCount();
        //open(name, append);
    }
    
    /**
     * Opens a file, with the specified name, for overwriting or appending.
     * @param name name of file to be opened
     * @param append whether the file is to be opened in append mode
     */
    //private native void open(String name, boolean append)
    //    throws FileNotFoundException;
    
    /**
     * The associated channel, initalized lazily.
     */
    private FileChannel channel;

    /**
     * Writes the specified byte to this file output stream. Implements
     * the <code>write</code> method of <code>OutputStream</code>.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void write(int b) throws IOException {
        // fileWriteBegin always return null? http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/misc/IoTrace.java
        Object traceContext = IoTrace.fileWriteBegin(path); 
        int bytesWritten = 0;
        try {
            file.write(b, append);
            bytesWritten = 1;
        } finally {
            // fileWriteEnd always return ? http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/misc/IoTrace.java
            IoTrace.fileWriteEnd(traceContext, bytesWritten);
        }
    }
    
    /**
     * Writes the specified byte to this file output stream.
     *
     * @param   b   the byte to be written.
     * @param   append   {@code true} if the write operation first
     *     advances the position to the end of file
     */
    //private void write(int b, boolean append) throws IOException {
        
    //}
    
    /**
     * Returns the unique {@link java.nio.channels.FileChannel FileChannel}
     * object associated with this file output stream. </p>
     *
     * <p> The initial {@link java.nio.channels.FileChannel#position()
     * </code>position<code>} of the returned channel will be equal to the
     * number of bytes written to the file so far unless this stream is in
     * append mode, in which case it will be equal to the size of the file.
     * Writing bytes to this stream will increment the channel's position
     * accordingly.  Changing the channel's position, either explicitly or by
     * writing, will change this stream's file position.
     *
     * @return  the file channel associated with this file output stream
     *
     */
    public FileChannel getChannel() {
        synchronized (this) {
            if (channel == null) {
                channel = FileChannelImpl.open(fd, path, false, true, append, this);

                /*
                 * Increment fd's use count. Invoking the channel's close()
                 * method will result in decrementing the use count set for
                 * the channel.
                 */
                //fd.incrementAndGetUseCount();
            }
            return channel;
        }
    }
    
    /**
     * Closes this file output stream and releases any system resources
     * associated with this stream. This file output stream may no longer
     * be used for writing bytes.
     *
     * <p> If this stream has an associated channel then the channel is closed
     * as well.
     *
     * @exception  IOException  if an I/O error occurs.
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
             * Decrement FD use count associated with the channel
             * The use count is incremented whenever a new channel
             * is obtained from this stream.
             */
            //  fd.decrementAndGetUseCount();
            channel.close();
        }

        /*
         * Decrement FD use count associated with this stream
         */
        //int useCount = fd.decrementAndGetUseCount();

        /*
         * If FileDescriptor is still in use by another stream, the finalizer
         * will not close it.
         */
        //if ((useCount <= 0) || !isRunningFinalize()) {
        //    close0();
        //}
    }

}
