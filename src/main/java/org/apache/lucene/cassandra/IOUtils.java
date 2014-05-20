package org.apache.lucene.cassandra;

import java.io.IOException;

import org.apache.lucene.util.ThreadInterruptedException;

public class IOUtils {

    public static void closeWhileHandlingException(IOException priorE,
            RandomAccessFile file) throws IOException {
//        org.apache.lucene.util.IOUtils.closeWhileHandlingException(priorE, file.getFile());
        
    }
    
    public static void closeWhileHandlingException(IOException priorE, CassandraRandomAccessFile file) throws IOException {
        
    }

    /**
     * Ensure that any writes to the given file is written to the storage device that contains it.
     * @param fileToSync the file to fsync
     * @param isDir if true, the given file is a directory (we open for read and ignore IOExceptions,
     *  because not all file systems and operating systems allow to fsync on a directory)
     *  
     *  Note: using implementation from lucene 4.6, lucene 4.8 using filechannel which need more
     *  TODO . 
     */
    public static void fsync(File fileToSync, boolean isDir) throws IOException {
        boolean success = false;
        int retryCount = 0;
        IOException exc = null;
        while (!success && retryCount < 5) {
          retryCount++;
          RandomAccessFile file = null;
          try {
            try {
              if (fileToSync.getAbsolutePath() == null) {
                success = true;
                continue;
              }
              file = fileToSync.getRandomAccessFile(fileToSync, "rw");
//              file = new RandomAccessFile(fullFile, "rw");
              file.getFDsync();
              success = true;
            } finally {
              if (file != null)
                file.close();
            }
          } catch (IOException ioe) {
            if (exc == null)
              exc = ioe;
            try {
              // Pause 5 msec
              Thread.sleep(5);
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
            }
          }
        }
        if (!success)
          // Throw original exception
          throw exc;
        
    }
    
    public static void fsync(CassandraFile fileToSync, boolean isDir) throws IOException {
        boolean success = false;
        int retryCount = 0;
        IOException exc = null;
        while (!success && retryCount < 5) {
            retryCount++;
            CassandraRandomAccessFile file = null;
            try {
                try {
                    //file = new CassandraRandomAccessFile(fullFile, "rw");
                    file = new CassandraRandomAccessFile(fileToSync, fileToSync.getMode(), true, fileToSync.getKeyspace(), fileToSync.getColumnFamily(), fileToSync.getBlockSize());
                    // public CassandraRandomAccessFile(CassandraFile path, String mode, boolean frameMode, String keyspace, String columnFamily) {
                    file.getFDsync();
                    success = true;
                } finally {
                    if (file != null)
                        file.close();
                }
            } catch (IOException ioe) {
                if (exc == null)
                    exc = ioe;
                try {
                    // Pause 5 msec
                    Thread.sleep(5);
                } catch (InterruptedException ie) {
                    throw new ThreadInterruptedException(ie);
                }
            }
        }
        if (!success)
            // Throw original exception
            throw exc;
        
    }

}
