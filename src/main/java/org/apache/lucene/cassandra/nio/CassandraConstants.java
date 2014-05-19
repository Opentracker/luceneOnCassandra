package org.apache.lucene.cassandra.nio;

public class CassandraConstants {

    static final int O_RDONLY = 0;
    static final int O_WRONLY = 1;
    static final int O_RDWR = 2;
    
    static final int S_IRUSR = 256;
    static final int S_IRGRP = 32;
    static final int S_IROTH = 4;
    static final int S_IWGRP = 16;
    static final int S_IWUSR = 128;
    static final int S_IWOTH = 2;
    static final int S_IXUSR = 64;
    static final int S_IXGRP = 8;
    static final int S_IXOTH = 1;
    
    static final int O_TRUNC = 0x200;
    static final int O_APPEND = 0x400;
    static final int O_CREAT = 0x40;
    static final int O_EXCL = 0x80;
    static final int O_NOFOLLOW = 0x20000;
    static final int O_DSYNC = 0x1000;
    static final int O_SYNC = 0x101000;
    
    static final int ENOENT = 2;
    static final int EACCES = 13;
    static final int EEXIST = 17;
    static final int EISDIR = 21;
    static final int ELOOP = 40;
    
}
