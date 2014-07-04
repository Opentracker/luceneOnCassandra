package org.apache.lucene.cassandra.nio;

import java.io.IOException;

import org.apache.lucene.cassandra.FileDescriptor;

// http://grepcode.com/file/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/nio/ch/FileDispatcherImpl.java#FileDispatcherImpl
class FileDispatcherImpl extends FileDispatcher {
    
    /**
     * Indicates if the dispatcher should first advance the file position
     * to the end of file when writing.
     */
    private final boolean append;

    FileDispatcherImpl(boolean append) {
        this.append = append;
    }

    @Override
    int force(FileDescriptor fd, boolean metaData) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int truncate(FileDescriptor fd, long size) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    long size(FileDescriptor fd) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    int lock(FileDescriptor fd, boolean blocking, long pos, long size,
            boolean shared) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    void release(FileDescriptor fd, long pos, long size) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    FileDescriptor duplicateForMapping(FileDescriptor fd) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    int read(FileDescriptor fd, long address, int len) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    long readv(FileDescriptor fd, long address, int len) throws IOException {
        return readv0(fd, address, len);
    }
    
    static native long readv0(FileDescriptor fd, long address, int len)
            throws IOException;

}
