package org.apache.lucene.cassandra.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.lucene.cassandra.FileDescriptor;

import sun.misc.IoTrace;

// http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/nio/ch/FileChannelImpl.java#FileChannelImpl
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

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the
     * given buffers.
     *
     * <p> Bytes are read starting at this channel's current file position, and
     * then the file position is updated with the number of bytes actually
     * read.  Otherwise this method behaves exactly as specified in the {@link
     * ScatteringByteChannel} interface.  </p>
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length)
            throws IOException {
        if ((offset < 0) || (length < 0) || (offset > dsts.length - length))
            throw new IndexOutOfBoundsException();
        ensureOpen();
        if (!readable)
            throw new NonReadableChannelException();
        synchronized (positionLock) {
          long n = 0;
          int ti = -1;
          Object traceContext = IoTrace.fileReadBegin(path);
          try {
              begin();
              ti = threads.add();
              if (!isOpen())
                  return 0;
              do {
                  n = IOUtil.read(fd, dsts, offset, length, nd);
              } while ((n == IOStatus.INTERRUPTED) && isOpen());
              return IOStatus.normalize(n);
          } finally {
              threads.remove(ti);
              IoTrace.fileReadEnd(traceContext, n > 0 ? n : 0);
              end(n > 0);
              assert IOStatus.check(n);
          }
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified by the {@link WritableByteChannel}
     * interface. </p>
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        ensureOpen();
        if (!writable)
            throw new NonWritableChannelException();
        synchronized (positionLock) {
            int n = 0;
            int ti = -1;
            Object traceContext = IoTrace.fileWriteBegin(path);
            try {
                begin();
                ti = threads.add();
                if (!isOpen())
                    return 0;
                do {
                    n = IOUtil.write(fd, src, -1, nd);
                } while ((n == IOStatus.INTERRUPTED) && isOpen());
                return IOStatus.normalize(n);
            } finally {
                threads.remove(ti);
                end(n > 0);
                IoTrace.fileWriteEnd(traceContext, n > 0 ? n : 0);
                assert IOStatus.check(n);
            }
        }
    }

    /**
     * Writes a sequence of bytes to this channel from a subsequence of the
     * given buffers.
     *
     * <p> Bytes are written starting at this channel's current file position
     * unless the channel is in append mode, in which case the position is
     * first advanced to the end of the file.  The file is grown, if necessary,
     * to accommodate the written bytes, and then the file position is updated
     * with the number of bytes actually written.  Otherwise this method
     * behaves exactly as specified in the {@link GatheringByteChannel}
     * interface.  </p>
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length)
            throws IOException {
        if ((offset < 0) || (length < 0) || (offset > srcs.length - length))
            throw new IndexOutOfBoundsException();
        ensureOpen();
        if (!writable)
            throw new NonWritableChannelException();
        synchronized (positionLock) {
            long n = 0;
            int ti = -1;
            Object traceContext = IoTrace.fileWriteBegin(path);
            try {
                begin();
                ti = threads.add();
                if (!isOpen())
                    return 0;
                do {
                    n = IOUtil.write(fd, srcs, offset, length, nd);
                } while ((n == IOStatus.INTERRUPTED) && isOpen());
                return IOStatus.normalize(n);
            } finally {
                threads.remove(ti);
                IoTrace.fileWriteEnd(traceContext, n > 0 ? n : 0);
                end(n > 0);
                assert IOStatus.check(n);
            }
        }
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

    /**
     * @see FileChannel#force(boolean)
     */
    @Override
    public void force(boolean metaData) throws IOException {
        ensureOpen();
        int rv = -1;
        int ti = -1;
        try {
            begin();
            ti = threads.add();
            if (!isOpen())
                return;
            do {
                rv = nd.force(fd, metaData);
            } while ((rv == IOStatus.INTERRUPTED) && isOpen());
        } finally {
            threads.remove(ti);
            end(rv > -1);
            assert IOStatus.check(rv);
        }
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

    /**
     * Reads a sequence of bytes from this channel into the given buffer,
     * starting at the given file position.
     *
     * <p> This method works in the same manner as the {@link
     * #read(ByteBuffer)} method, except that bytes are read starting at the
     * given file position rather than at the channel's current position.  This
     * method does not modify this channel's position.  If the given position
     * is greater than the file's current size then no bytes are read.  </p>
     *
     * @param  dst
     *         The buffer into which bytes are to be transferred
     *
     * @param  position
     *         The file position at which the transfer is to begin;
     *         must be non-negative
     *
     * @return  The number of bytes read, possibly zero, or <tt>-1</tt> if the
     *          given position is greater than or equal to the file's current
     *          size
     *
     * @throws  IllegalArgumentException
     *          If the position is negative
     *
     * @throws  NonReadableChannelException
     *          If this channel was not opened for reading
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     *
     * @throws  AsynchronousCloseException
     *          If another thread closes this channel
     *          while the read operation is in progress
     *
     * @throws  ClosedByInterruptException
     *          If another thread interrupts the current thread
     *          while the read operation is in progress, thereby
     *          closing the channel and setting the current thread's
     *          interrupt status
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        if (dst == null)
            throw new NullPointerException();
        if (position < 0)
            throw new IllegalArgumentException("Negative position");
        if (!readable)
            throw new NonReadableChannelException();
        ensureOpen();
        if (nd.needsPositionLock()) {
            synchronized (positionLock) {
                return readInternal(dst, position);
            }
        } else {
            return readInternal(dst, position);
        }
    }
    
    private int readInternal(ByteBuffer dst, long position) throws IOException {
        assert !nd.needsPositionLock() || Thread.holdsLock(positionLock);
        int n = 0;
        int ti = -1;
        Object traceContext = IoTrace.fileReadBegin(path);
        try {
            begin();
            ti = threads.add();
            if (!isOpen())
                return -1;
            do {
                n = IOUtil.read(fd, dst, position, nd);
            } while ((n == IOStatus.INTERRUPTED) && isOpen());
            return IOStatus.normalize(n);
        } finally {
            threads.remove(ti);
            IoTrace.fileReadEnd(traceContext, n > 0 ? n : 0);
            end(n > 0);
            assert IOStatus.check(n);
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer,
     * starting at the given file position.
     *
     * <p> This method works in the same manner as the {@link
     * #write(ByteBuffer)} method, except that bytes are written starting at
     * the given file position rather than at the channel's current position.
     * This method does not modify this channel's position.  If the given
     * position is greater than the file's current size then the file will be
     * grown to accommodate the new bytes; the values of any bytes between the
     * previous end-of-file and the newly-written bytes are unspecified.  </p>
     *
     * @param  src
     *         The buffer from which bytes are to be transferred
     *
     * @param  position
     *         The file position at which the transfer is to begin;
     *         must be non-negative
     *
     * @return  The number of bytes written, possibly zero
     *
     * @throws  IllegalArgumentException
     *          If the position is negative
     *
     * @throws  NonWritableChannelException
     *          If this channel was not opened for writing
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     *
     * @throws  AsynchronousCloseException
     *          If another thread closes this channel
     *          while the write operation is in progress
     *
     * @throws  ClosedByInterruptException
     *          If another thread interrupts the current thread
     *          while the write operation is in progress, thereby
     *          closing the channel and setting the current thread's
     *          interrupt status
     *
     * @throws  IOException
     *          If some other I/O error occurs
     */
    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (src == null)
            throw new NullPointerException();
        if (position < 0)
            throw new IllegalArgumentException("Negative position");
        if (!writable)
            throw new NonWritableChannelException();
        ensureOpen();
        if (nd.needsPositionLock()) {
            synchronized (positionLock) {
                return writeInternal(src, position);
            }
        } else {
            return writeInternal(src, position);
        }
    }
    
    private int writeInternal(ByteBuffer src, long position) throws IOException {
        assert !nd.needsPositionLock() || Thread.holdsLock(positionLock);
        int n = 0;
        int ti = -1;
        Object traceContext = IoTrace.fileWriteBegin(path);
        try {
            begin();
            ti = threads.add();
            if (!isOpen())
                return -1;
            do {
                n = IOUtil.write(fd, src, position, nd);
            } while ((n == IOStatus.INTERRUPTED) && isOpen());
            return IOStatus.normalize(n);
        } finally {
            threads.remove(ti);
            end(n > 0);
            IoTrace.fileWriteEnd(traceContext, n > 0 ? n : 0);
            assert IOStatus.check(n);
        }
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
