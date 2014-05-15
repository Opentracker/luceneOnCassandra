package org.apache.lucene.cassandra;

import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class CassandraFileModeAttribute {
    static final int ALL_READWRITE =
            CassandraConstants.S_IRUSR | CassandraConstants.S_IWUSR |
            CassandraConstants.S_IRGRP | CassandraConstants.S_IWGRP |
            CassandraConstants.S_IROTH | CassandraConstants.S_IWOTH;
    
    static int toCassandraMode(Set<PosixFilePermission> perms) {
        int mode = 0;
        for (PosixFilePermission perm: perms) {
            if (perm == null)
                throw new NullPointerException();
            switch (perm) {
                case OWNER_READ :     mode |= CassandraConstants.S_IRUSR; break;
                case OWNER_WRITE :    mode |= CassandraConstants.S_IWUSR; break;
                case OWNER_EXECUTE :  mode |= CassandraConstants.S_IXUSR; break;
                case GROUP_READ :     mode |= CassandraConstants.S_IRGRP; break;
                case GROUP_WRITE :    mode |= CassandraConstants.S_IWGRP; break;
                case GROUP_EXECUTE :  mode |= CassandraConstants.S_IXGRP; break;
                case OTHERS_READ :    mode |= CassandraConstants.S_IROTH; break;
                case OTHERS_WRITE :   mode |= CassandraConstants.S_IWOTH; break;
                case OTHERS_EXECUTE : mode |= CassandraConstants.S_IXOTH; break;
            }
        }
        return mode;
    }

    public static int toCassandraMode(int defaultMode, FileAttribute<?>... attrs) {
        int mode = defaultMode;
        for (FileAttribute<?> attr: attrs) {
            String name = attr.name();
            if (!name.equals("posix:permissions") && !name.equals("unix:permissions")) {
                throw new UnsupportedOperationException("'" + attr.name() +
                   "' not supported as initial attribute");
            }
            mode = toCassandraMode((Set<PosixFilePermission>)attr.value());
        }
        return mode;
    }
}
