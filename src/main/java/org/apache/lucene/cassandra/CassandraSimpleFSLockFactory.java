package org.apache.lucene.cassandra;

import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockReleaseFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSimpleFSLockFactory extends CassandraFSLockFactory {
    
    private static Logger logger = LoggerFactory.getLogger(CassandraSimpleFSLockFactory.class);
    
    String keyspace; 
    String columnFamily;
    IOContext mode;
    boolean frameMode;
    int blockSize;

    /**
     * Create a CassandraSimpleFSLockFactory instance, with null (unset)
     * lock directory. When you pass this factory to a {@link FSDirectory}
     * subclass, the lock directory is automatically set to the
     * directory itself. Be sure to create one instance for each directory
     * your create!
     */
    /*
    public CassandraSimpleFSLockFactory() {
      this((CassandraFile) null);
    }
    */

    /**
     * Instantiate using the provided directory (as a File instance).
     * @param lockDir where lock files should be created.
     */
    /*
    public CassandraSimpleFSLockFactory(CassandraFile lockDir) {
      setLockDir(lockDir);
    }
    */

  /**
     * Instantiate using the provided directory name (String).
     * @param lockDirName where lock files should be created.
     */
    public CassandraSimpleFSLockFactory(CassandraFile file, String lockDirName, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize) {
      //setLockDir(new CassandraFile(lockDirName));
        logger.trace("path {} lockDirName {}", Util.getCassandraPath(file), lockDirName);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.mode = mode;
        this.frameMode = frameMode;
        this.blockSize = blockSize;
        setLockDir(new CassandraFile(Util.getCassandraPath(file), lockDirName, this.mode, this.frameMode, this.keyspace, this.columnFamily, this.blockSize));
    }

    @Override
    public Lock makeLock(String lockName) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
      return new CassandraSimpleFSLock(lockDir, lockName, mode, frameMode, keyspace, columnFamily, blockSize);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
      if (lockDir.exists()) {
        if (lockPrefix != null) {
          lockName = lockPrefix + "-" + lockName;
        }
        //CassandraFile lockFile = new CassandraFile(lockDir, lockName);
        CassandraFile lockFile = new CassandraFile(Util.getCassandraPath(lockDir), lockName, mode, true, keyspace, columnFamily, this.blockSize);
        if (lockFile.exists() && !lockFile.delete()) {
            if (lockFile != null) {
                lockFile.close();
            }
            throw new IOException("Cannot delete " + lockFile);
        }
        lockFile.close();
      }
    }
  }

  class CassandraSimpleFSLock extends Lock {

    CassandraFile lockFile;
    CassandraFile lockDir;

    public CassandraSimpleFSLock(CassandraFile lockDir, String lockFileName, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize) {
      this.lockDir = lockDir;
      //lockFile = new CassandraFile(lockDir, lockFileName);
      lockFile = new CassandraFile(Util.getCassandraPath(lockDir), lockFileName, mode, frameMode, keyspace, columnFamily, blockSize);
    }

    public boolean obtain() throws IOException {

      // Ensure that lockDir exists and is a directory:
      if (!lockDir.exists()) {
        if (!lockDir.mkdirs())
          throw new IOException("Cannot create directory: " +
                                lockDir.getAbsolutePath());
      } else if (!lockDir.isDirectory()) {
        throw new IOException("Found regular file where directory expected: " + 
                              lockDir.getAbsolutePath());
      }
      return lockFile.createNewFile();
    }

    public void release() throws LockReleaseFailedException {
      if (lockFile.exists() && !lockFile.delete())
        throw new LockReleaseFailedException("failed to delete " + lockFile);
    }

    public boolean isLocked() {
      return lockFile.exists();
    }

    public String toString() {
      return "CassandraSimpleFSLock@" + lockFile;
    }
  }
