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

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.IOUtils;

/**
 * <p>Implements {@link LockFactory} using native OS file
 * locks.  Note that because this LockFactory relies on
 * java.nio.* APIs for locking, any problems with those APIs
 * will cause locking to fail.  Specifically, on certain NFS
 * environments the java.nio.* locks will fail (the lock can
 * incorrectly be double acquired) whereas {@link
 * SimpleFSLockFactory} worked perfectly in those same
 * environments.  For NFS based access to an index, it's
 * recommended that you try {@link SimpleFSLockFactory}
 * first and work around the one limitation that a lock file
 * could be left when the JVM exits abnormally.</p>
 *
 * <p>The primary benefit of {@link CassandraNativeFSLockFactory} is
 * that locks (not the lock file itsself) will be properly
 * removed (by the OS) if the JVM has an abnormal exit.</p>
 * 
 * <p>Note that, unlike {@link SimpleFSLockFactory}, the existence of
 * leftover lock files in the filesystem is fine because the OS
 * will free the locks held against these files even though the
 * files still remain. Lucene will never actively remove the lock
 * files, so although you see them, the index may not be locked.</p>
 *
 * <p>Special care needs to be taken if you change the locking
 * implementation: First be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index. Be sure to do the LockFactory change on all Lucene
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

public class CassandraNativeFSLockFactory extends CassandraFSLockFactory {
    
    String keyspace; 
    String columnFamily;
    IOContext mode;
    boolean frameMode;
    int blockSize;

  /**
   * Create a CassandraNativeFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
  public CassandraNativeFSLockFactory() {
    this((CassandraFile) null);
  }

  /**
   * Create a CassandraNativeFSLockFactory instance, storing lock
   * files into the specified lockDirName:
   *
   * @param lockDirName where lock files are created.
   */
  /*
  public CassandraNativeFSLockFactory(String lockDirName) {
    this(new File(lockDirName));
  }
  */

  /**
   * Create a CassandraNativeFSLockFactory instance, storing lock
   * files into the specified lockDir:
   * 
   * @param lockDir where lock files are created.
   */
  public CassandraNativeFSLockFactory(CassandraFile lockDir) {
    setLockDir(lockDir);
  }
  
  public CassandraNativeFSLockFactory(CassandraFile path, String fileName,
          IOContext mode, boolean b, String keyspace, String columnFamily,
          int blockSize) {
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
      this.mode = mode;
      this.frameMode = frameMode;
      this.blockSize = blockSize;
      setLockDir(new CassandraFile(Util.getCassandraPath(path), fileName, this.mode, this.frameMode, this.keyspace, this.columnFamily, this.blockSize));
  }

@Override
  public synchronized Lock makeLock(String lockName) {
    if (lockPrefix != null)
      lockName = lockPrefix + "-" + lockName;
    return new CassandraNativeFSLock(lockDir, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    makeLock(lockName).close();
  }
}

class CassandraNativeFSLock extends Lock {

  private FileChannel channel;
  private FileLock lock;
  private CassandraFile path;
  private CassandraFile lockDir;

  public CassandraNativeFSLock(CassandraFile lockDir, String lockFileName) {
    this.lockDir = lockDir;
    path = new CassandraFile(Util.getCassandraPath(lockDir), lockFileName, lockDir.getMode(), true, lockDir.getKeyspace(), lockDir.getColumnFamily(), lockDir.getBlockSize());
  }

  @Override
  public synchronized boolean obtain() throws IOException {

    if (lock != null) {
      // Our instance is already locked:
      return false;
    }

    // Ensure that lockDir exists and is a directory.
    if (!lockDir.exists()) {
      if (!lockDir.mkdirs())
        throw new IOException("Cannot create directory: " +
            lockDir.getAbsolutePath());
    } else if (!lockDir.isDirectory()) {
      // TODO: NoSuchDirectoryException instead?
      throw new IOException("Found regular file where directory expected: " + 
          lockDir.getAbsolutePath());
    }
    
    channel = FileChannel.open(path.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    boolean success = false;
    try {
      lock = channel.tryLock();
      success = lock != null;
    } catch (IOException | OverlappingFileLockException e) {
      // At least on OS X, we will sometimes get an
      // intermittent "Permission Denied" IOException,
      // which seems to simply mean "you failed to get
      // the lock".  But other IOExceptions could be
      // "permanent" (eg, locking is not supported via
      // the filesystem).  So, we record the failure
      // reason here; the timeout obtain (usually the
      // one calling us) will use this as "root cause"
      // if it fails to get the lock.
      failureReason = e;
    } finally {
      if (!success) {
        try {
          IOUtils.closeWhileHandlingException(channel);
        } finally {
          channel = null;
        }
      }
    }
    return lock != null;
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (lock != null) {
        lock.release();
        lock = null;
      }
    } finally {
      if (channel != null) {
        channel.close();
        channel = null;
      }
    }
  }

  @Override
  public synchronized boolean isLocked() {
    // The test for is isLocked is not directly possible with native file locks:
    
    // First a shortcut, if a lock reference in this instance is available
    if (lock != null) return true;
    
    // Look if lock file is present; if not, there can definitely be no lock!
    if (!path.exists()) return false;
    
    // Try to obtain and release (if was locked) the lock
    try {
      boolean obtained = obtain();
      if (obtained) close();
      return !obtained;
    } catch (IOException ioe) {
      return false;
    }    
  }

  @Override
  public String toString() {
    return "CassandraNativeFSLock@" + path;
  }
}
