package org.apache.lucene.cassandra;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;

public class CassandraException extends Exception {
    
    private int errno;
    private String msg;

    CassandraException(String msg) {
        this.errno = 0;
        this.msg = msg;
    }

    CassandraException(int errno) {
        this.errno = errno;
        this.msg = null;
    }
    
    /**
     * Map well known errors to specific exceptions where possible; otherwise
     * return more general FileSystemException.
     */
    private IOException translateToIOException(String file, String other) {
        // created with message rather than errno
        if (msg != null)
            return new IOException(msg);

        // handle specific cases
        if (errno() == CassandraConstants.EACCES)
            return new AccessDeniedException(file, other, null);
        if (errno() == CassandraConstants.ENOENT)
            return new NoSuchFileException(file, other, null);
        if (errno() == CassandraConstants.EEXIST)
            return new FileAlreadyExistsException(file, other, null);

        // fallback to the more general exception
        return new FileSystemException(file, other, errorString());
    }
    
    void rethrowAsIOException(CassandraPath file, CassandraPath other) throws IOException {
        String a = (file == null) ? null : file.getPathForExceptionMessage();
        String b = (other == null) ? null : other.getPathForExceptionMessage();
        IOException x = translateToIOException(a, b);
        throw x;
    }

    public void rethrowAsIOException(CassandraPath file) throws IOException {
        rethrowAsIOException(file, null);
    }

    int errno() {
        return errno;
    }

    public void setError(int errno) {
        this.errno = errno;
        this.msg = null;
    }
    
    String errorString() {
        if (msg != null) {
            return msg;
        } else {
            return new String(CassandraNativeDispatcher.strerror(errno()));
        }
    }

}
