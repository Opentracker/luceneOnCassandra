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
