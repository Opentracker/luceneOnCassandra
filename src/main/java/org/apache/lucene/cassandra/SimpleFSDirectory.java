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


import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A straightforward implementation of {@link FSDirectory}
 *  using java.io.RandomAccessFile.  However, this class has
 *  poor concurrent performance (multiple threads will
 *  bottleneck) as it synchronizes when multiple threads
 *  read from the same file.  It's usually better to use
 *  {@link NIOFSDirectory} or {@link MMapDirectory} instead. */
public class SimpleFSDirectory extends FSDirectory {
    
  private static Logger logger = LoggerFactory.getLogger(SimpleFSDirectory.class);
  
  /** Create a new SimpleFSDirectory for the named location.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  public SimpleFSDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);
  }
  
  /** Create a new SimpleFSDirectory for the named location and {@link NativeFSLockFactory}.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public SimpleFSDirectory(File path) throws IOException {
    super(path, null);
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    logger.info("openInput name {}", name);
    final File path = directory.get(directory, name);
    //RandomAccessFile raf = new ACassandraRandomAccessFile(path, "r");
    RandomAccessFile raf = (RandomAccessFile) path;
    return new SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + path.getPath() + "\")", raf, context);
  }

  @Override
  public IndexInputSlicer createSlicer(final String name,
      final IOContext context) throws IOException {
    ensureOpen();
    logger.info("createSlicer name {}", name);
    File directory = getDirectory();
    final File file = directory.get(directory, name);
    final RandomAccessFile descriptor = file.getRandomAccessFile(file, "r");

    return new IndexInputSlicer() {

      @Override
      public void close() throws IOException {
          logger.info("close");
        descriptor.close();
      }

      @Override
      public IndexInput openSlice(String sliceDescription, long offset, long length) {
          logger.info("openSlice {} file {}", sliceDescription, file.getAbsolutePath());
        return new SimpleFSIndexInput("SimpleFSIndexInput(" + sliceDescription + " in path=\"" + file.getPath() + "\" slice=" + offset + ":" + (offset+length) + ")", descriptor, offset,
            length, BufferedIndexInput.bufferSize(context));
      }

      @Override
      public IndexInput openFullSlice() {
          logger.info("openFullSlice ");
        try {
          return openSlice("full-slice", 0, descriptor.length());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  /**
   * Reads bytes with {@link RandomAccessFile#seek(long)} followed by
   * {@link RandomAccessFile#read(byte[], int, int)}.  
   */
  protected static class SimpleFSIndexInput extends BufferedIndexInput {
    /**
     * The maximum chunk size is 8192 bytes, because {@link RandomAccessFile} mallocs
     * a native buffer outside of stack if the read buffer size is larger.
     */
    private static final int CHUNK_SIZE = 8192;
  
    /** the file channel we will read from */
    protected final RandomAccessFile file;
    /** is this instance a clone and hence does not own the file to close it */
    boolean isClone = false;
    /** start offset: non-zero in the slice case */
    protected final long off;
    /** end offset (start+length) */
    protected final long end;
        
    public SimpleFSIndexInput(String resourceDesc, RandomAccessFile file, IOContext context) throws IOException {
        super(resourceDesc, context);
        this.file = file; 
        this.off = 0L;
        this.end = file.length();
    }
    
    public SimpleFSIndexInput(String resourceDesc, RandomAccessFile file, long off, long length, int bufferSize) {
        super(resourceDesc, bufferSize);
        this.file = file;
        this.off = off;
        this.end = off + length;
        this.isClone = true;
    }
    
    @Override
    public void close() throws IOException {
      if (!isClone) {
        file.close();
      }
    }
    
    @Override
    public SimpleFSIndexInput clone() {
      SimpleFSIndexInput clone = (SimpleFSIndexInput)super.clone();
      clone.isClone = true;
      return clone;
    }
    
    @Override
    public final long length() {
      return end - off;
    }
  
    /** IndexInput methods */
    @Override
    protected void readInternal(byte[] b, int offset, int len)
         throws IOException {
        logger.trace("readInternal offset {} len {}", offset, len);
        logger.trace("readInternal byte", b);
      synchronized (file) {
        long position = off + getFilePointer();
        logger.trace("position: {}", position);
        file.seek(position);
        int total = 0;

        if (position + len > end) {
          logger.error("throwing");
          throw new EOFException("read past EOF: " + this);
        }

        try {
          while (total < len) {
            final int toRead = Math.min(CHUNK_SIZE, len - total);
            final int i = file.read(b, offset + total, toRead);
            if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
             logger.error("throwing");
             throw new EOFException("read past EOF: " + this + " off: " + offset + " len: " + len + " total: " + total + " chunkLen: " + toRead + " end: " + end);
            }
            logger.trace("i > 0: " + (i>0));

            assert i > 0 : "RandomAccessFile.read with non zero-length toRead must always read at least one byte";
            total += i;
          }
          logger.trace("total == len: " + (total == len));
          assert total == len;
        } catch (IOException ioe) {
          logger.error("throwing");
          throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
      }
    }
  
    @Override
    protected void seekInternal(long position) {
        logger.info("seeking internal with position {}", position);
    }
  }
}