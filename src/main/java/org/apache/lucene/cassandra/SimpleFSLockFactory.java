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

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockReleaseFailedException;




/*

1c1
< package org.apache.lucene.store;
---
> package org.apache.lucene.cassandra;
20d19
< import java.io.File;
22a22,24
> import org.apache.lucene.store.Lock;
> import org.apache.lucene.store.LockReleaseFailedException;
> 
75c77
<   /**
---
> /**
115d116
<   @Override
131d131
<   @Override
137d136
<   @Override
142d140
<   @Override


*/


/**
 * <p>Implements {@link LockFactory} using {@link
 * File#createNewFile()}.</p>
 *
 * <p><b>NOTE:</b> the <a target="_top"
 * href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/File.html#createNewFile()">javadocs
 * for <code>File.createNewFile</code></a> contain a vague
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
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link VerifyingLockFactory}, {@link
 * LockVerifyServer} and {@link LockStressTest}.</p>
 *
 * @see LockFactory
 */

public class SimpleFSLockFactory extends FSLockFactory {

  /**
   * Create a SimpleFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
  public SimpleFSLockFactory() {
    this((File) null);
  }

  /**
   * Instantiate using the provided directory (as a File instance).
   * @param lockDir where lock files should be created.
   */
  public SimpleFSLockFactory(File lockDir) {
    setLockDir(lockDir);
  }

/**
   * Instantiate using the provided directory name (String).
   * @param lockDirName where lock files should be created.
   */
//  public SimpleFSLockFactory(String lockDirName) {
//    setLockDir(new File(lockDirName));
//      directory.
//  }

  @Override
  public Lock makeLock(String lockName) {
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    return new SimpleFSLock(lockDir, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    if (lockDir.exists()) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
//      File lockFile = new File(lockDir, lockName);
      File lockFile = lockDir.get(lockDir, lockName);
      if (lockFile.exists() && !lockFile.delete()) {
        throw new IOException("Cannot delete " + lockFile);
      }
    }
  }
}

class SimpleFSLock extends Lock {

  File lockFile;
  File lockDir;

  public SimpleFSLock(File lockDir, String lockFileName) {
    this.lockDir = lockDir;
//    lockFile = new File(lockDir, lockFileName);
    lockFile = lockDir.get(lockDir, lockFileName);
  }

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
    return "SimpleFSLock@" + lockFile;
  }
}