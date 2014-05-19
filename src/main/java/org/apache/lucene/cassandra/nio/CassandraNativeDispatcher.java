package org.apache.lucene.cassandra.nio;

public class CassandraNativeDispatcher {
    
    protected CassandraNativeDispatcher() { }

    public static int open(CassandraPath path, int oflags, int mode) {
        // TODO Auto-generated method stub
        return 0;
    }
    
    /**
     * unlink(const char* path)
     */
    static void unlink(CassandraPath path) throws CassandraException {
        /*
        NativeBuffer buffer = copyToNativeBuffer(path);
        try {
            unlink0(buffer.address());
        } finally {
            buffer.release();
        }
        */
    }
    private static native void unlink0(long pathAddress) throws CassandraException;
    
    /**
     * unlinkat(int dfd, const char* path, int flag)
     */
    static void unlinkat(int dfd, byte[] path, int flag) throws CassandraException {
        /*
        NativeBuffer buffer = NativeBuffers.asNativeBuffer(path);
        try {
            unlinkat0(dfd, buffer.address(), flag);
        } finally {
            buffer.release();
        }
        */
    }
    private static native void unlinkat0(int dfd, long pathAddress, int flag)
        throws CassandraException;
    
    /**
     * int openat(int dfd, const char* path, int oflag, mode_t mode)
     */
    static int openat(int dfd, byte[] path, int flags, int mode) throws CassandraException {
        /*
        NativeBuffer buffer = NativeBuffers.asNativeBuffer(path);
        try {
            return openat0(dfd, buffer.address(), flags, mode);
        } finally {
            buffer.release();
        }
        */
        return 0;
    }
    private static native int openat0(int dfd, long pathAddress, int flags, int mode)
        throws CassandraException;

    /**
     * char* strerror(int errnum)
     */
    static native byte[] strerror(int errnum);

}
