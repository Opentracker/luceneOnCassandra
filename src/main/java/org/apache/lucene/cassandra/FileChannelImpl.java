package org.apache.lucene.cassandra;

import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class FileChannelImpl extends FileChannel {
    
    // File descriptor
    private FileDescriptor fd;

    // File access mode (immutable)
    private boolean writable;
    private boolean readable;
    private boolean appending;
    
    // Required to prevent finalization of creating stream (immutable)
    private Object parent;
    
    private FileChannelImpl(FileDescriptor fd, boolean readable,
            boolean writable, Object parent, boolean append) {
        this.fd = fd;
        this.readable = readable;
        this.writable = writable;
        this.parent = parent;
        this.appending = append;
    }

    public static FileChannel open(FileDescriptor fd, boolean readable,
            boolean writeable, boolean append, Object parent) {
        return new FileChannelImpl(fd, readable, writeable, parent, append);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        
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
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void implCloseChannel() throws IOException {
        // TODO Auto-generated method stub
        
    }

}
