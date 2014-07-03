package org.apache.lucene.cassandra.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.lucene.cassandra.FileDescriptor;

import sun.misc.IoTrace;

public class FileChannelImpl extends FileChannel {
    
    // Memory allocation size for mapping buffers
    // TODO Fix below;
    //private static final long allocationGranularity;

    // Used to make native read and write calls
    private final FileDispatcher nd;
    
    // File descriptor
    private FileDescriptor fd;

    // File access mode (immutable)
    private boolean writable;
    private boolean readable;
    private boolean append;
    
    // Required to prevent finalization of creating stream (immutable)
    private Object parent;
    
    // The path of the referenced file (null if the parent stream is created with a file descriptor)
    private final String path;
    
    // Thread-safe set of IDs of native threads, for signalling
    private final NativeThreadSet threads = new NativeThreadSet(2);
    
    // Lock for operations involving position and size
    private final Object positionLock = new Object();
    
    private FileChannelImpl(FileDescriptor fd, String path, boolean readable,
            boolean writable, boolean append, Object parent) {
        this.fd = fd;
        this.readable = readable;
        this.writable = writable;
        this.append = append;
        this.parent = parent;
        this.path = path;
        this.nd = new FileDispatcherImpl(append);
    }

    // Used by CassandraRandomAccessFile.getChannel()
    public static FileChannel open(FileDescriptor fd, String path,
                                   boolean readable, boolean writable,
                                   Object parent)
    {
        return new FileChannelImpl(fd, path, readable, writable, false, parent);
    }

    // Used by FileOutputStream.getChannel
    public static FileChannel open(FileDescriptor fd, String path,
                                   boolean readable, boolean writable,
                                   boolean append, Object parent)
    {
        return new FileChannelImpl(fd, path, readable, writable, append, parent);
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p> Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface. </p>
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        ensureOpen();
        if (!readable)
            throw new NonReadableChannelException();
        synchronized (positionLock) {
            int n = 0;
            int ti = -1;
            Object traceContext = IoTrace.fileReadBegin(path);
            try {
                begin();
                ti = threads.add();
                if (!isOpen())
                    return 0;
                do {
                    n = IOUtil.read(fd, dst, -1, nd);
                } while ((n == IOStatus.INTERRUPTED) && isOpen());
                return IOStatus.normalize(n);
            } finally  {
                threads.remove(ti);
                IoTrace.fileReadEnd(traceContext, n > 0 ? n : 0);
                end(n > 0);
                assert IOStatus.check(n);
            }
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        // TODO implement
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        // TODO implement
        return 0;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        // TODO implement
        return 0;
    }

    @Override
    public long position() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long size() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // TODO implement
        
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        // TODO implement
        return 0;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        // TODO implement
        return 0;
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FileLock lock(long position, long size, boolean shared)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared)
            throws IOException {
        return new FileLock(this, position, size, shared) {

            @Override
            public void release() throws IOException {
            }

            @Override
            public boolean isValid() {
                return true;
            }
        };
    }

    @Override
    protected void implCloseChannel() throws IOException {
        // TODO Auto-generated method stub
        
    }
    
    private void ensureOpen() throws IOException {
        if (!isOpen())
            throw new ClosedChannelException();
    }

}
