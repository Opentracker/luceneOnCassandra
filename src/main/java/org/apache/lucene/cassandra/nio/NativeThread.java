package org.apache.lucene.cassandra.nio;

class NativeThread {
    
    static long current() {
        // return 0 to ensure that async close of blocking sockets will close
        // the underlying socket.
        return 0;
    }
    
    static void signal(long nt) { }

}
