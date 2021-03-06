package org.apache.lucene.cassandra;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.io.SyncFailedException;

import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRandomAccessFile implements Serializable {
    
    CassandraFile file;
    
    private static Logger logger = LoggerFactory
            .getLogger(CassandraRandomAccessFile.class);

    /**
     * 
     * @param path
     * @param mode  rw
     */
    public CassandraRandomAccessFile(CassandraFile path, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int bufferSize) {
        logger.trace("called CassandraRandomAccessFile {} {}", path.getName(), mode);
        //file = new CassandraFile(path.getFile(), path.getName(), mode);
        file =  new CassandraFile(Util.getCassandraPath(path), Util.getFileName(path), mode, frameMode, keyspace, columnFamily, bufferSize);
    }

    public long length() throws IOException {
        logger.trace("called length {} of this file {}", file.length(), this.file.getName());
        return file.length();
    }

    public void close() throws IOException {
        logger.trace("called close");
        file.close();
    }

    // Writes n bytes from the specified byte array starting at offset to this file.
    public void write(byte[] b, int offset, int n) throws IOException {
        logger.trace("called write");
        file.write(b, offset, n);
    }

    /* Sets the file-pointer offset, measured from the beginning of this file, at which 
     * the next read or write occurs. The offset may be set beyond the end of the 
     * file. Setting the offset beyond the end of the file does not change the file 
     * length. The file length will change only by writing after the offset has been 
     * set beyond the end of the file. 
     */
    public void seek(long pos) throws IOException {
        logger.trace("called seek {}", pos);
        file.seek(pos);
    }

    // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#setLength%28long%29
    public void setLength(long length) {
        logger.trace("called setLength");
        file.setLength(length);
    }

    //http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#read%28byte[],%20int,%20int%29
    public int read(byte[] b, int off, int len) throws IOException { 
        String debug = String.format("reading a length of %s from this file %s into byte array  %s with starting offset %s", len, file.getName() , Util.debugBytesToHex(b), off);
        //logger.trace(debug);
        int read = file.read(b, off, len);
        logger.info("read {} off {} ", read, off);
        logger.info("Util.bytesToHex({})", Util.bytesToHex(b));
        return read;
    }
    
    // don't actually need this method.
    public Closeable getFile() {
        logger.trace("called getFile");
        return file;        
    }

    /**
     * Force all system buffers to synchronize with the underlying
     * device.  This method returns after all modified data and
     * attributes of this FileDescriptor have been written to the
     * relevant device(s).  In particular, if this FileDescriptor
     * refers to a physical storage medium, such as a file in a file
     * system, sync will not return until all in-memory modified copies
     * of buffers associated with this FileDescriptor have been
     * written to the physical medium.
     *
     * sync is meant to be used by code that requires physical
     * storage (such as a file) to be in a known state  For
     * example, a class that provided a simple transaction facility
     * might use sync to ensure that all changes to a file caused
     * by a given transaction were recorded on a storage medium.
     *
     * sync only affects buffers downstream of this FileDescriptor.  If
     * any in-memory buffering is being done by the application (for
     * example, by a BufferedOutputStream object), those buffers must
     * be flushed into the FileDescriptor (for example, by invoking
     * OutputStream.flush) before that data will be affected by sync.
     *
     * @exception SyncFailedException
     *        Thrown when the buffers cannot be flushed,
     *        or because the system cannot guarantee that all the
     *        buffers have been synchronized with physical media.
     * @since     JDK1.1
     */
    public void getFDsync() throws IOException{
        logger.trace("called getFDsync");
        
        FileDescriptor fd = file.getFD();
        
        String existingFD = FileDescriptorUtils.toString(fd);
        
        ColumnOrientedFile cof = new ColumnOrientedFile(file.getCassandraClient());
        FileDescriptor cassandraFD = cof.getFileDescriptor(file.getName(), file.getBlockSize());

        String cassandraFDString = FileDescriptorUtils.toString(cassandraFD);
        
        if (!cassandraFDString.equals(existingFD)) {
            logger.error("existing fd {}", FileDescriptorUtils.toString(fd));
            logger.error("cassandra fd {}", FileDescriptorUtils.toString(cassandraFD));
        }
        
        cof.setFileDescriptor(file.getName(), fd);
    }
    
    /**
     * Tests if this file descriptor object is valid.
     *
     * @return  <code>true</code> if the file descriptor object represents a
     *          valid, open file, socket, or other active I/O connection;
     *          <code>false</code> otherwise.
     */
    public boolean getFDvalid() {
        logger.trace("called getFDvalid ");
        
        if (file.getFD() != null) {
            return true;
        }
        
        return false;
    }

}
