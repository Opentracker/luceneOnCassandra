package org.apache.lucene.cassandra.nio;

import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import org.apache.lucene.cassandra.FileDescriptor;

import sun.misc.JavaIOFileDescriptorAccess;
import sun.misc.SharedSecrets;

import static org.apache.lucene.cassandra.nio.CassandraNativeDispatcher.*;
import static org.apache.lucene.cassandra.nio.CassandraConstants.*;

// http://grepcode.com/file_/repository.grepcode.com/java/root/jdk/openjdk/7u40-b43/sun/nio/fs/UnixChannelFactory.java/?v=source
public class CassandraChannelFactory {
    
    private static final JavaIOFileDescriptorAccess fdAccess =
            SharedSecrets.getJavaIOFileDescriptorAccess();
    
    protected CassandraChannelFactory() {
    }
    
    /**
     * Represents the flags from a user-supplied set of open options.
     */
    protected static class Flags {
        boolean read;
        boolean write;
        boolean append;
        boolean truncateExisting;
        boolean noFollowLinks;
        boolean create;
        boolean createNew;
        boolean deleteOnClose;
        boolean sync;
        boolean dsync;

        static Flags toFlags(Set<? extends OpenOption> options) {
            Flags flags = new Flags();
            for (OpenOption option: options) {
                if (option instanceof StandardOpenOption) {
                    switch ((StandardOpenOption)option) {
                        case READ : flags.read = true; break;
                        case WRITE : flags.write = true; break;
                        case APPEND : flags.append = true; break;
                        case TRUNCATE_EXISTING : flags.truncateExisting = true; break;
                        case CREATE : flags.create = true; break;
                        case CREATE_NEW : flags.createNew = true; break;
                        case DELETE_ON_CLOSE : flags.deleteOnClose = true; break;
                        case SPARSE : /* ignore */ break;
                        case SYNC : flags.sync = true; break;
                        case DSYNC : flags.dsync = true; break;
                        default: throw new UnsupportedOperationException();
                    }
                    continue;
                }
                if (option == LinkOption.NOFOLLOW_LINKS) {
                    flags.noFollowLinks = true;
                    continue;
                }
                if (option == null)
                    throw new NullPointerException();
               throw new UnsupportedOperationException();
            }
            return flags;
        }
    }

    static FileChannel newFileChannel(CassandraPath path,
            Set<? extends OpenOption> options, int mode) throws CassandraException {
        return newFileChannel(-1, path, null, options, mode);
    }

    /**
     * Constructs a file channel by opening a file using a dfd/path pair
     */
    static FileChannel newFileChannel(int dfd,
                                      CassandraPath path,
                                      String pathForPermissionCheck,
                                      Set<? extends OpenOption> options,
                                      int mode)
        throws CassandraException
    {
        Flags flags = Flags.toFlags(options);

        // default is reading; append => writing
        if (!flags.read && !flags.write) {
            if (flags.append) {
                flags.write = true;
            } else {
                flags.read = true;
            }
        }

        // validation
        if (flags.read && flags.append)
            throw new IllegalArgumentException("READ + APPEND not allowed");
        if (flags.append && flags.truncateExisting)
            throw new IllegalArgumentException("APPEND + TRUNCATE_EXISTING not allowed");

        // TODO This need to be fix.
        FileDescriptor fdObj = open(dfd, path, pathForPermissionCheck, flags, mode);
        return FileChannelImpl.open(fdObj, path.toString(), flags.read, flags.write, flags.append, null);
    }
    
    /**
     * Opens file based on parameters and options, returning a FileDescriptor
     * encapsulating the handle to the open file.
     * 
     * TODO test me for cassandra.
     */
    protected static FileDescriptor open(int dfd,
                                         CassandraPath path,
                                         String pathForPermissionCheck,
                                         Flags flags,
                                         int mode)
        throws CassandraException
    {
        // map to oflags
        int oflags;
        if (flags.read && flags.write) {
            oflags = O_RDWR;
        } else {
            oflags = (flags.write) ? O_WRONLY : O_RDONLY;
        }
        if (flags.write) {
            if (flags.truncateExisting)
                oflags |= O_TRUNC;
            if (flags.append)
                oflags |= O_APPEND;

            // create flags
            if (flags.createNew) {
                byte[] pathForSysCall = path.asByteArray();

                // throw exception if file name is "." to avoid confusing error
                if ((pathForSysCall[pathForSysCall.length-1] == '.') &&
                    (pathForSysCall.length == 1 ||
                    (pathForSysCall[pathForSysCall.length-2] == '/')))
                {
                    throw new CassandraException(EEXIST);
                }
                oflags |= (O_CREAT | O_EXCL);
            } else {
                if (flags.create)
                    oflags |= O_CREAT;
            }
        }

        // follow links by default
        boolean followLinks = true;
        if (!flags.createNew && (flags.noFollowLinks || flags.deleteOnClose)) {
            followLinks = false;
            oflags |= O_NOFOLLOW;
        }

        if (flags.dsync)
            oflags |= O_DSYNC;
        if (flags.sync)
            oflags |= O_SYNC;

        // permission check before we open the file
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            if (pathForPermissionCheck == null)
                pathForPermissionCheck = path.getPathForPermissionCheck();
            if (flags.read)
                sm.checkRead(pathForPermissionCheck);
            if (flags.write)
                sm.checkWrite(pathForPermissionCheck);
            if (flags.deleteOnClose)
                sm.checkDelete(pathForPermissionCheck);
        }

        int fd;
        try {
            if (dfd >= 0) {
                fd = openat(dfd, path.asByteArray(), oflags, mode);
            } else {
                fd = CassandraNativeDispatcher.open(path, oflags, mode);
            }
        } catch (CassandraException x) {
            // Linux error can be EISDIR or EEXIST when file exists
            if (flags.createNew && (x.errno() == EISDIR)) {
                x.setError(EEXIST);
            }

            // handle ELOOP to avoid confusing message
            if (!followLinks && (x.errno() == ELOOP)) {
                x = new CassandraException(x.getMessage() + " (NOFOLLOW_LINKS specified)");
            }

            throw x;
        }

        // unlink file immediately if delete on close. The spec is clear that
        // an implementation cannot guarantee to unlink the correct file when
        // replaced by an attacker after it is opened.
        if (flags.deleteOnClose) {
            try {
                if (dfd >= 0) {
                    unlinkat(dfd, path.asByteArray(), 0);
                } else {
                    unlink(path);
                }
            } catch (CassandraException ignore) {
                // best-effort
            }
        }

        // create java.io.FileDescriptor
        // TODO fix me below.
        FileDescriptor fdObj = new FileDescriptor(null, 16384);
        // TODO fix me below
        //fdAccess.set(fdObj, fd);
        return fdObj;
    }

}
