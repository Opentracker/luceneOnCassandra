package org.apache.lucene.cassandra;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockReleaseFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Implements {@link LockFactory} using {@link
 * File#createNewFile()}.</p>
 *
 * <p><b>NOTE:</b> the {@linkplain File#createNewFile() javadocs
 * for <code>File.createNewFile()</code>} contain a vague
 * yet spooky warning about not using the API for file
 * locking.  This warning was added due to <a target="_top"
 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183">this
 * bug</a>, and in fact the only known problem with using
 * this API for locking is that the Lucene write lock may
 * not be released when the JVM exits abnormally.</p>

 * <p>When this happens, a {@link LockObtainFailedException}
 * is hit when trying to create a writer, in which case you
 * need to explicitly clear the lock file first.  You can
 * either manually remove the file, or use the {@link
 * org.apache.lucene.index.IndexWriter#unlock(Directory)}
 * API.  But, first be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change all Lucene
 * instances and clean up all leftover lock files before starting
 * the new configuration for the first time. Different implementations
 * can not work together!</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 *
 * @see LockFactory
 */

public class CassandraSimpleFSLockFactory extends CassandraFSLockFactory {
    private static Logger logger = LoggerFactory.getLogger(CassandraSimpleFSLockFactory.class);
    
    String keyspace; 
    String columnFamily;
    IOContext mode;
    boolean frameMode;
    int blockSize;


  /**
   * Create a SimpleFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
    /*
  public CassandraSimpleFSLockFactory() {
    this((File) null);
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
//  public SimpleFSLockFactory(String lockDirName) {
//    setLockDir(new File(lockDirName));
//  }
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
//      File lockFile = new File(lockDir, lockName);
      CassandraFile lockFile = new CassandraFile(Util.getCassandraPath(lockDir), lockName, mode, true, keyspace, columnFamily, this.blockSize);
      if (lockFile.exists() && !lockFile.delete()) {
        throw new IOException("Cannot delete " + lockFile);
      }
    }
  }
}

class CassandraSimpleFSLock extends Lock {

  CassandraFile lockFile;
  CassandraFile lockDir;

  public CassandraSimpleFSLock(CassandraFile lockDir, String lockFileName, IOContext mode, boolean frameMode, String keyspace, String columnFamily, int blockSize) {
    this.lockDir = lockDir;
//    lockFile = new File(lockDir, lockFileName);
    lockFile = new CassandraFile(Util.getCassandraPath(lockDir), lockFileName, mode, frameMode, keyspace, columnFamily, blockSize);
  }

  @Override
  public boolean obtain() throws IOException {

    // Ensure that lockDir exists and is a directory:
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
                              lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      // TODO: NoSuchDirectoryException instead?
      throw new IOException("Found regular file where directory expected: " + 
                            lockDir.getAbsolutePath());
    }
    
    try {
      return lockFile.createNewFile();
    } catch (IOException ioe) {
      // On Windows, on concurrent createNewFile, the 2nd process gets "access denied".
      // In that case, the lock was not aquired successfully, so return false.
      // We record the failure reason here; the obtain with timeout (usually the
      // one calling us) will use this as "root cause" if it fails to get the lock.
      failureReason = ioe;
      return false;
    }
  }

  @Override
  public void close() throws LockReleaseFailedException {
    if (lockFile.exists() && !lockFile.delete()) {
      throw new LockReleaseFailedException("failed to delete " + lockFile);
    }
  }

  @Override
  public boolean isLocked() {
    return lockFile.exists();
  }

  @Override
  public String toString() {
    return "SimpleFSLock@" + lockFile;
  }
}
