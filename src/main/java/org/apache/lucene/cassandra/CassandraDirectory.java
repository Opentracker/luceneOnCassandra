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


import org.apache.lucene.util.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoSuchDirectoryException;
import org.apache.lucene.util.ThreadInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.synchronizedSet;

/**
 * Base class for Directory implementations that store index
 * files in the file system.  
 * <a name="subclasses"/>
 * There are currently three core
 * subclasses:
 *
 * <ul>
 *
 *  <li> {@link SimpleCassandraDirectory} is a straightforward
 *       implementation using java.io.CassandraRandomAccessFile.
 *       However, it has poor concurrent performance
 *       (multiple threads will bottleneck) as it
 *       synchronizes when multiple threads read from the
 *       same file.
 *
 *  <li> {@link NIOCassandraDirectory} uses java.nio's
 *       FileChannel's positional io when reading to avoid
 *       synchronization when reading from the same file.
 *       Unfortunately, due to a Windows-only <a
 *       href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6265734">Sun
 *       JRE bug</a> this is a poor choice for Windows, but
 *       on all other platforms this is the preferred
 *       choice. Applications using {@link Thread#interrupt()} or
 *       {@link Future#cancel(boolean)} should use
 *       {@link SimpleCassandraDirectory} instead. See {@link NIOCassandraDirectory} java doc
 *       for details.
 *        
 *        
 *
 *  <li> {@link MMapDirectory} uses memory-mapped IO when
 *       reading. This is a good choice if you have plenty
 *       of virtual memory relative to your index size, eg
 *       if you are running on a 64 bit JRE, or you are
 *       running on a 32 bit JRE but your index sizes are
 *       small enough to fit into the virtual memory space.
 *       Java has currently the limitation of not being able to
 *       unmap files from user code. The files are unmapped, when GC
 *       releases the byte buffers. Due to
 *       <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038">
 *       this bug</a> in Sun's JRE, MMapDirectory's {@link IndexInput#close}
 *       is unable to close the underlying OS file handle. Only when
 *       GC finally collects the underlying objects, which could be
 *       quite some time later, will the file handle be closed.
 *       This will consume additional transient disk usage: on Windows,
 *       attempts to delete or overwrite the files will result in an
 *       exception; on other platforms, which typically have a &quot;delete on
 *       last close&quot; semantics, while such operations will succeed, the bytes
 *       are still consuming space on disk.  For many applications this
 *       limitation is not a problem (e.g. if you have plenty of disk space,
 *       and you don't rely on overwriting files on Windows) but it's still
 *       an important limitation to be aware of. This class supplies a
 *       (possibly dangerous) workaround mentioned in the bug report,
 *       which may fail on non-Sun JVMs.
 *       
 *       Applications using {@link Thread#interrupt()} or
 *       {@link Future#cancel(boolean)} should use
 *       {@link SimpleCassandraDirectory} instead. See {@link MMapDirectory}
 *       java doc for details.
 * </ul>
 *
 * Unfortunately, because of system peculiarities, there is
 * no single overall best implementation.  Therefore, we've
 * added the {@link #open} method, to allow Lucene to choose
 * the best CassandraDirectory implementation given your
 * environment, and the known limitations of each
 * implementation.  For users who have no reason to prefer a
 * specific implementation, it's best to simply use {@link
 * #open}.  For all others, you should instantiate the
 * desired implementation directly.
 *
 * <p>The locking implementation is by default {@link
 * NativeFSLockFactory}, but can be changed by
 * passing in a custom {@link LockFactory} instance.
 *
 * @see Directory
 */
public abstract class CassandraDirectory extends BaseDirectory {
    
    private static Logger logger = LoggerFactory.getLogger(CassandraDirectory.class);
    protected String keyspace = null;
    protected String columnFamily = null;
    protected int blockSize;
    protected int bufferSize;
    protected IOContext mode;

  /**
   * Default read chunk size: 8192 bytes (this is the size up to which the JDK
     does not allocate additional arrays while reading/writing)
     @deprecated This constant is no longer used since Lucene 4.5.
   */
  @Deprecated
  public static final int DEFAULT_READ_CHUNK_SIZE = 8192;

  protected final CassandraFile directory; // The underlying filesystem directory
  protected final Set<String> staleFiles = synchronizedSet(new HashSet<String>()); // Files written, but not yet sync'ed
  private int chunkSize = DEFAULT_READ_CHUNK_SIZE;

  // returns the canonical version of the directory, creating it if it doesn't exist.
  private static CassandraFile getCanonicalPath(CassandraFile file, IOContext mode, String keyspace, String columnFamily, int blockSize) throws IOException {
    return new CassandraFile(Util.getCassandraPath(file), Util.getFileName(file), mode, true, keyspace, columnFamily, blockSize);
  }

  /** Create a new CassandraDirectory for the named location (ctor for subclasses).
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  protected CassandraDirectory(CassandraFile path, IOContext mode, LockFactory lockFactory, String keyspace, String columnFamily, int blockSize, int bufferSize) throws IOException {
    // new ctors use always NativeFSLockFactory as default:
    if (lockFactory == null) {
      lockFactory = new CassandraSimpleFSLockFactory(path, Util.getFileName(path), mode, true, keyspace, columnFamily, blockSize);
      // TODO this exist in lucene 4.8.0, make it work.
      //lockFactory = new CassandraNativeFSLockFactory(path, Util.getFileName(path), mode, true, keyspace, columnFamily, blockSize);
    }
    this.keyspace = keyspace;
    this.columnFamily = columnFamily;
    this.blockSize = blockSize;
    this.bufferSize = bufferSize;
    this.mode = mode;
    directory = getCanonicalPath(path, mode, keyspace, columnFamily, blockSize);
    logger.trace("path is {}", path.getName());

    if (directory.exists() && !directory.isDirectory())
      throw new NoSuchDirectoryException("file '" + directory + "' exists but is not a directory");

    setLockFactory(lockFactory);

  }

  /** Creates an CassandraDirectory instance, trying to pick the
   *  best implementation given the current environment.
   *  The directory returned uses the {@link NativeFSLockFactory}.
   *
   *  <p>Currently this returns {@link MMapDirectory} for most Solaris
   *  and Windows 64-bit JREs, {@link NIOCassandraDirectory} for other
   *  non-Windows JREs, and {@link SimpleCassandraDirectory} for other
   *  JREs on Windows. It is highly recommended that you consult the
   *  implementation's documentation for your platform before
   *  using this method.
   *
   * <p><b>NOTE</b>: this method may suddenly change which
   * implementation is returned from release to release, in
   * the event that higher performance defaults become
   * possible; if the precise implementation is important to
   * your application, please instantiate it directly,
   * instead. For optimal performance you should consider using
   * {@link MMapDirectory} on 64 bit JVMs.
   *
   * <p>See <a href="#subclasses">above</a> */
  /*
  public static CassandraDirectory open(CassandraFile path) throws IOException {
    return open(path, null, "lucene1", "index1", 16384, 16384);
  }
  */

  /** Just like {@link #open(File)}, but allows you to
   *  also specify a custom {@link LockFactory}. */
  /*
  public static CassandraDirectory open(CassandraFile path, LockFactory lockFactory, String keyspace, String columnFamily) throws IOException {
      return new SimpleCassandraDirectory(path, lockFactory, keyspace, columnFamily, 16384, 16384);
  }
  */
  
  public static CassandraDirectory open(CassandraFile path, IOContext mode, LockFactory lockFactory, String keyspace, String columnFamily, int blockSize, int bufferSize) throws IOException {
      logger.info("initializing CassandraDirectory path {} lockFactory {}", path.getName(), lockFactory);
      /*
      if ((Constants.WINDOWS || Constants.SUN_OS || Constants.LINUX)
                   && Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
         return new MMapDirectory(path, lockFactory);
      } else if (Constants.WINDOWS) {
         return new SimpleFSDirectory(path, lockFactory);
      } else {
         return new NIOFSDirectory(path, lockFactory);
      }
      */
    return new SimpleCassandraDirectory(path, mode, lockFactory, keyspace, columnFamily, blockSize, bufferSize);
  }

  @Override
  public void setLockFactory(LockFactory lockFactory) throws IOException {
    super.setLockFactory(lockFactory);

    // for filesystem based LockFactory, delete the lockPrefix, if the locks are placed
    // in index dir. If no index dir is given, set ourselves
    if (lockFactory instanceof CassandraFSLockFactory) {
      final CassandraFSLockFactory lf = (CassandraFSLockFactory) lockFactory;
      final CassandraFile dir = lf.getLockDir();
      // if the lock factory has no lockDir set, use the this directory as lockDir
      if (dir == null) {
          logger.trace("dir is null");
        lf.setLockDir(directory);
        lf.setLockPrefix(null);
      } else if (dir.getCanonicalPath().equals(directory.getCanonicalPath())) {
          logger.trace("they are equal null");
        lf.setLockPrefix(null);
      }
    }
    
    logger.info("done set lock factory");

  }
  
  /** Lists all files (not subdirectories) in the
   *  directory.  This method never returns null (throws
   *  {@link IOException} instead).
   *
   *  @throws NoSuchDirectoryException if the directory
   *   does not exist, or does exist but is not a
   *   directory.
   *  @throws IOException if list() returns null */
    public static String[] listAll(CassandraFile dir) throws IOException {
        if (!dir.exists())
            throw new NoSuchDirectoryException("directory '" + dir.getName() + "' does not exist");
        else if (!dir.isDirectory())
            throw new NoSuchDirectoryException("file '" + dir.getName() + "' exists but is not a directory");

        // TODO Exclude subdirs
        /*
        String[] result = dir.list(new java.io.FilenameFilter() {
            @Override
            public boolean accept(java.io.File dir, String name) {
                return !new java.io.File(dir, name).isDirectory();
            }
        });
        */
        String[] result = dir.list();
        
        logger.info("listAll result length " + result.length);
        
        for( int i = 0; i <= result.length - 1; i++) { 
            logger.info(result[i]);
        }

        return result;
    }

  /** Lists all files (not subdirectories) in the
   * directory.
   * @see #listAll(File) */
  @Override
  public String[] listAll() throws IOException {
    ensureOpen();
    logger.trace("listAll new");
    return listAll(directory);
  }

  /** Returns true if a file with the given name exists. */
  @Override
  public boolean fileExists(String name) {
    ensureOpen();
    logger.trace("fileExists {}", name);
    CassandraFile file = new CassandraFile(Util.getCassandraPath(directory), name, IOContext.READ, true, this.keyspace, this.columnFamily, this.blockSize);
    boolean isFileExists = file.exists(); 
    file.close();
    return isFileExists;
  }

  /** Returns the length in bytes of a file in the directory. */
  @Override
  public long fileLength(String name) throws IOException {
    ensureOpen();
    logger.trace("fileLength {}", name);
    CassandraFile file = new CassandraFile(Util.getCassandraPath(directory), name, IOContext.READ, true, keyspace, columnFamily, this.blockSize);
    final long len = file.length();
    file.close();
    if (len == 0 && !file.exists()) {
      logger.error(String.format("file %s with len %s is exists %s", file.getName(), len, file.exists()));
      throw new FileNotFoundException(name);
    } else {
      return len;
    }
  }

  /** Removes an existing file in the directory. */
  @Override
  public void deleteFile(String name) throws IOException {
    ensureOpen();
    logger.trace("deleteFile {}", name);
    // do not create the descriptor file during deleting, just does not make sense.
    // hence, mode is set to r only.
    CassandraFile file = new CassandraFile(Util.getCassandraPath(directory), name, IOContext.DEFAULT, true, keyspace, columnFamily, this.blockSize);
    boolean isDeleted = file.delete();
    file.close();
    if (!isDeleted)
      throw new IOException("Cannot delete " + file);
    staleFiles.remove(name);
  }

  /** Creates an IndexOutput for the file with the given name. */
  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    ensureOpen();
    logger.trace("createOutput {}", name);
    ensureCanWrite(name);
    return new FSIndexOutput(this, name);
  }

  protected void ensureCanWrite(String name) throws IOException {
      logger.trace("ensureCanWrite {}", name);
    if (!directory.exists())
      if (!directory.mkdirs())
        throw new IOException("Cannot create directory: " + directory);

    CassandraFile file = new CassandraFile(Util.getCassandraPath(directory), name, IOContext.DEFAULT, true, keyspace, columnFamily, this.blockSize);
    if (file.exists() && !file.delete()) {          // delete existing, if any
      file.close();
      throw new IOException("Cannot overwrite: " + file);
    }
    file.close();
  }

  protected void onIndexOutputClosed(FSIndexOutput io) {
      logger.trace("onIndexOutputClosed {}", io.name);
      staleFiles.add(io.name);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();
    Set<String> toSync = new HashSet<>(names);
    toSync.retainAll(staleFiles);

    for (String name : toSync) {
      fsync(name);
    }
    
    // fsync the directory itsself, but only if there was any file fsynced before
    // (otherwise it can happen that the directory does not yet exist)!
    if (!toSync.isEmpty()) {
      IOUtils.fsync(directory, true);
    }
    
    staleFiles.removeAll(toSync);
  }

  @Override
  public String getLockID() {
    ensureOpen();
    String dirName;                               // name to be hashed
    try {
      dirName = directory.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e.toString(), e);
    }

    int digest = 0;
    for(int charIDX=0;charIDX<dirName.length();charIDX++) {
      final char ch = dirName.charAt(charIDX);
      digest = 31 * digest + ch;
    }
    return "lucene-" + Integer.toHexString(digest);
  }

  /** Closes the store to future operations. */
  @Override
  public synchronized void close() {
    isOpen = false;
  }

  /** @return the underlying filesystem directory */
  public CassandraFile getDirectory() {
    ensureOpen();
    logger.trace("getDirectory ");
    return directory;
  }

  /** For debug output. */
  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "@" + directory + " lockFactory=" + getLockFactory();
  }

  /**
   * This setting has no effect anymore.
   * @deprecated This is no longer used since Lucene 4.5.
   */
  @Deprecated
  public final void setReadChunkSize(int chunkSize) {
      logger.trace("setReadChunkSize {} ", chunkSize);
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("chunkSize must be positive");
    }
    this.chunkSize = chunkSize;
  }

  /**
   * This setting has no effect anymore.
   * @deprecated This is no longer used since Lucene 4.5.
   */
  @Deprecated
  public final int getReadChunkSize() {
      logger.trace("getReadChunkSize {} ", chunkSize);
    return chunkSize;
  }
  
    /**
     * Writes output with {@link RandomAccessFile#write(byte[], int, int)}
     */
    protected static class FSIndexOutput extends BufferedIndexOutput {
        /**
         * The maximum chunk size is 8192 bytes, because
         * {@link RandomAccessFile} mallocs a native buffer outside of stack if
         * the write buffer size is larger.
         */
        private static final int CHUNK_SIZE = 8192;

        private final CassandraDirectory parent;

        private final String name;

        private final CassandraRandomAccessFile file;

        private volatile boolean isOpen; // remember if the file is open, so
                                         // that we don't try to close it more
                                         // than once

        public FSIndexOutput(CassandraDirectory parent, String name)
                throws IOException {
            super(CHUNK_SIZE);
            logger.trace("initializing FSIndexOutput name {}", name);
            this.parent = parent;
            this.name = name;
            file = new CassandraRandomAccessFile(new CassandraFile(Util.getCassandraPath(parent.directory), name, parent.mode, true, parent.keyspace, parent.columnFamily, parent.blockSize), parent.mode, true, parent.keyspace, parent.columnFamily, parent.blockSize);
            isOpen = true;
        }

        @Override
        protected void flushBuffer(byte[] b, int offset, int size)
                throws IOException {
            logger.trace("flushBuffer");
            assert isOpen;
            while (size > 0) {
                final int toWrite = Math.min(CHUNK_SIZE, size);
                file.write(b, offset, toWrite);
                offset += toWrite;
                size -= toWrite;
            }
            assert size == 0;
        }

        @Override
        public void close() throws IOException {
            logger.trace("closed");
            parent.onIndexOutputClosed(this);
            // only close the file if it has not been closed yet
            if (isOpen) {
                IOException priorE = null;
                try {
                    super.close();
                } catch (IOException ioe) {
                    priorE = ioe;
                } finally {
                    isOpen = false;
                    IOUtils.closeWhileHandlingException(priorE, file);
                }
            }
        }

        /** Random-access methods */
        @Override
        public void seek(long pos) throws IOException {
            logger.trace("seek {}", pos);
            super.seek(pos);
            file.seek(pos);
        }

        @Override
        public long length() throws IOException {
            logger.trace("length ");
            return file.length();
        }

        @Override
        public void setLength(long length) throws IOException {
            logger.trace("setLength {}", length);
            file.setLength(length);
        }
    }

    protected void fsync(String name) throws IOException {
        logger.trace("fsync {}", name);
        CassandraFile fullFile = new CassandraFile(Util.getCassandraPath(directory), name, mode, true, keyspace, columnFamily, blockSize);
        IOUtils.fsync(fullFile, false);
    }
}