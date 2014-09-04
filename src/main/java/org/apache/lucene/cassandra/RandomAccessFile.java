/**
 * 
 */
package org.apache.lucene.cassandra;

import java.io.IOException;
import java.io.SyncFailedException;

/**
 * Instances of this class support both reading and writing to a random access
 * file. A random access file behaves like a large array of bytes stored in the
 * file system. There is a kind of cursor, or index into the implied array,
 * called the <em>file pointer</em>; input operations read bytes starting at the
 * file pointer and advance the file pointer past the bytes read. If the random
 * access file is created in read/write mode, then output operations are also
 * available; output operations write bytes starting at the file pointer and
 * advance the file pointer past the bytes written. Output operations that write
 * past the current end of the implied array cause the array to be extended. The
 * file pointer can be read by the <code>getFilePointer</code> method and set by
 * the <code>seek</code> method.
 * <p>
 * It is generally true of all the reading routines in this class that if
 * end-of-file is reached before the desired number of bytes has been read, an
 * <code>EOFException</code> (which is a kind of <code>IOException</code>) is
 * thrown. If any byte cannot be read for any reason other than end-of-file, an
 * <code>IOException</code> other than <code>EOFException</code> is thrown. In
 * particular, an <code>IOException</code> may be thrown if the stream has been
 * closed.
 * 
 */
public interface RandomAccessFile {

    /**
     * Returns the length of this file.
     * 
     * @return the length of this file, measured in bytes.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public long length() throws IOException;

    /**
     * Closes this random access file stream and releases any system resources
     * associated with the stream. A closed random access file cannot perform
     * input or output operations and cannot be reopened.
     * 
     * <p>
     * If this file has an associated channel then the channel is closed as
     * well.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     * 
     */
    public void close() throws IOException;

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this file.
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
    public void write(byte[] b, int offset, int len) throws IOException;

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
    public void seek(long pos) throws IOException;

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
     */
    public void setLength(long newLength) throws IOException;

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
    public int read(byte b[], int off, int len) throws IOException;

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
     */
    public void getFDsync() throws SyncFailedException, IOException;

    /**
     * Tests if this file descriptor object is valid.
     * 
     * @return <code>true</code> if the file descriptor object represents a
     *         valid, open file, socket, or other active I/O connection;
     *         <code>false</code> otherwise.
     */
    public boolean getFDvalid() throws IOException;
    
    
    public File getFile();

}