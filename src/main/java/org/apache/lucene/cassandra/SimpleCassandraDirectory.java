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

import org.apache.lucene.cassandra.SimpleFSDirectory.SimpleFSIndexInput;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A straightforward implementation of {@link CassandraDirectory}
 *  using java.io.RandomAccessFile.  However, this class has
 *  poor concurrent performance (multiple threads will
 *  bottleneck) as it synchronizes when multiple threads
 *  read from the same file.  It's usually better to use
 *  {@link NIOFSDirectory} or {@link MMapDirectory} instead. */
public class SimpleCassandraDirectory extends CassandraDirectory {

    private static Logger logger = LoggerFactory.getLogger(SimpleCassandraDirectory.class);
    
    private String keyspace = null;
    private String columnFamily = null;
    private int blockSize;
    private int bufferSize;
    
    public SimpleCassandraDirectory(CassandraFile path, IOContext mode, LockFactory lockFactory, String keyspace, String columnFamily, int blockSize, int bufferSize) throws IOException {        
        super(path, mode, lockFactory, keyspace, columnFamily, blockSize, bufferSize);
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.blockSize = blockSize;
        this.bufferSize = bufferSize;
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        ensureOpen();
        logger.trace("openInput name {} context {}", name, context); 
        final CassandraFile path = new CassandraFile(Util.getCassandraPath(directory), name, context, true, keyspace, columnFamily, blockSize);
        return new CassandraSimpleFSIndexInput("CassandraSimpleFSIndexInput(path=\"" + path.getPath() + "\")", path, context);
    }
    
    @Override
    public IndexInputSlicer createSlicer(final String name,
        final IOContext context) throws IOException {
      ensureOpen();
      logger.trace("createSlicer name {}", name);
      final CassandraFile file = new CassandraFile(Util.getCassandraPath(getDirectory()), name, context, true, keyspace, columnFamily, blockSize);
      //final CassandraRandomAccessFile descriptor = new CassandraRandomAccessFile(file, "r");
      final CassandraRandomAccessFile descriptor = new CassandraRandomAccessFile(file, context, true, keyspace, columnFamily, blockSize);
      return new IndexInputSlicer() {

        @Override
        public void close() throws IOException {
            logger.trace("close");
          descriptor.close();
        }

        @Override
        public IndexInput openSlice(String sliceDescription, long offset, long length) {
            logger.trace("openSlice {} file {}", sliceDescription, file.getName());
          return new CassandraSimpleFSIndexInput("CassandraSimpleFSIndexInput(" + sliceDescription + " in path=\"" + file.getPath() + "\" slice=" + offset + ":" + (offset+length) + ")", descriptor, offset,
              length, BufferedIndexInput.bufferSize(context));
        }

        @Override
        public IndexInput openFullSlice() {
          try {
              logger.trace("openFullSlice");
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
    protected static class CassandraSimpleFSIndexInput extends BufferedIndexInput {
      /**
       * The maximum chunk size is 8192 bytes, because {@link RandomAccessFile} mallocs
       * a native buffer outside of stack if the read buffer size is larger.
       */
      private static final int CHUNK_SIZE = 8192;
      
      /** the file channel we will read from */
      protected final CassandraRandomAccessFile file;
      /** is this instance a clone and hence does not own the file to close it */
      boolean isClone = false;
      /** start offset: non-zero in the slice case */
      protected final long off;
      /** end offset (start+length) */
      protected final long end;
    
      public CassandraSimpleFSIndexInput(String resourceDesc, CassandraFile path, IOContext context) throws IOException {
          super(resourceDesc, context);
          this.file = new CassandraRandomAccessFile(path, path.getMode(), true, path.getKeyspace(), path.getColumnFamily(), path.getBlockSize());
          this.off = 0L;
          this.end = file.length();
      }
      
      public CassandraSimpleFSIndexInput(String resourceDesc, CassandraRandomAccessFile file, long off, long length, int bufferSize) {
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
          logger.trace("readInternal {}", file.file.getName());
        synchronized (file) {
          long position = off + getFilePointer();
          logger.trace("position: {}", position);
          file.seek(position);
          int total = 0;

          if (position + len > end) {
              logger.trace("throwing");
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
