package org.apache.lucene.cassandra.nio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.cassandra.FileDescriptor;

import sun.nio.ch.DirectBuffer;

class IOUtil {
    
    /**
     * Max number of iovec structures that readv/writev supports
     */
    static final int IOV_MAX;

    static int read(FileDescriptor fd, ByteBuffer dst, long position,
            NativeDispatcher nd) throws IOException {
        if (dst.isReadOnly())
            throw new IllegalArgumentException("Read-only buffer");
        if (dst instanceof DirectBuffer)
            return readIntoNativeBuffer(fd, dst, position, nd);

        // Substitute a native buffer
        ByteBuffer bb = Util.getTemporaryDirectBuffer(dst.remaining());
        try {
            int n = readIntoNativeBuffer(fd, bb, position, nd);
            bb.flip();
            if (n > 0)
                dst.put(bb);
            return n;
        } finally {
            Util.offerFirstTemporaryDirectBuffer(bb);
        }
    }
    
    static long read(FileDescriptor fd, ByteBuffer[] bufs, int offset,
            int length, NativeDispatcher nd) throws IOException {
        IOVecWrapper vec = IOVecWrapper.get(length);

        boolean completed = false;
        int iov_len = 0;
        try {

            // Iterate over buffers to populate native iovec array.
            int count = offset + length;
            int i = offset;
            while (i < count && iov_len < IOV_MAX) {
                ByteBuffer buf = bufs[i];
                if (buf.isReadOnly())
                    throw new IllegalArgumentException("Read-only buffer");
                int pos = buf.position();
                int lim = buf.limit();
                assert (pos <= lim);
                int rem = (pos <= lim ? lim - pos : 0);

                if (rem > 0) {
                    vec.setBuffer(iov_len, buf, pos, rem);

                    // allocate shadow buffer to ensure I/O is done with direct
                    // buffer
                    if (!(buf instanceof DirectBuffer)) {
                        ByteBuffer shadow = Util.getTemporaryDirectBuffer(rem);
                        vec.setShadow(iov_len, shadow);
                        buf = shadow;
                        pos = shadow.position();
                    }

                    vec.putBase(iov_len, ((DirectBuffer) buf).address() + pos);
                    vec.putLen(iov_len, rem);
                    iov_len++;
                }
                i++;
            }
            if (iov_len == 0)
                return 0L;

            long bytesRead = nd.readv(fd, vec.address, iov_len);

            // Notify the buffers how many bytes were read
            long left = bytesRead;
            for (int j = 0; j < iov_len; j++) {
                ByteBuffer shadow = vec.getShadow(j);
                if (left > 0) {
                    ByteBuffer buf = vec.getBuffer(j);
                    int rem = vec.getRemaining(j);
                    int n = (left > rem) ? rem : (int) left;
                    if (shadow == null) {
                        int pos = vec.getPosition(j);
                        buf.position(pos + n);
                    } else {
                        shadow.limit(shadow.position() + n);
                        buf.put(shadow);
                    }
                    left -= n;
                }
                if (shadow != null)
                    Util.offerLastTemporaryDirectBuffer(shadow);
                vec.clearRefs(j);
            }

            completed = true;
            return bytesRead;

        } finally {
            // if an error occurred then clear refs to buffers and return any
            // shadow
            // buffers to cache
            if (!completed) {
                for (int j = 0; j < iov_len; j++) {
                    ByteBuffer shadow = vec.getShadow(j);
                    if (shadow != null)
                        Util.offerLastTemporaryDirectBuffer(shadow);
                    vec.clearRefs(j);
                }
            }
        }
    }
    
    private static int readIntoNativeBuffer(FileDescriptor fd, ByteBuffer bb,
            long position, NativeDispatcher nd) throws IOException {
        int pos = bb.position();
        int lim = bb.limit();
        assert (pos <= lim);
        int rem = (pos <= lim ? lim - pos : 0);

        if (rem == 0)
            return 0;
        int n = 0;
        if (position != -1) {
            n =
                    nd.pread(fd, ((DirectBuffer) bb).address() + pos, rem,
                            position);
        } else {
            n = nd.read(fd, ((DirectBuffer) bb).address() + pos, rem);
        }
        if (n > 0)
            bb.position(pos + n);
        return n;
    }
    
    static native int iovMax();
    
    static native void initIDs();
    
    static {
        // Note that IOUtil.initIDs is called from within Util.load.
        Util.load();
        IOV_MAX = iovMax();
    }

}
