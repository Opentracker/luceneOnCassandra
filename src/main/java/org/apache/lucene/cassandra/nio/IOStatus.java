package org.apache.lucene.cassandra.nio;

final class IOStatus {
    
    private IOStatus() {
        
    }
    
    static final int EOF = -1;                              // End of file
    static final int UNAVAILABLE = -2;              // Nothing available (non-blocking)
    static final int INTERRUPTED = -3;              // System call interrupted
    static final int UNSUPPORTED = -4;            // Operation not supported
    static final int THROWN = -5;                      // Exception thrown in JNI code
    static final int UNSUPPORTED_CASE = -6; // This case not supported
    
    static int normalize(int n) {
        if (n == UNAVAILABLE)
            return 0;
        return n;
    }
    
    static boolean check(int n) {
        return (n >= UNAVAILABLE);
    }
    
    static long normalize(long n) {
        if (n == UNAVAILABLE)
            return 0;
        return n;
    }
    
    static boolean check(long n) {
        return (n >= UNAVAILABLE);
    }

}
