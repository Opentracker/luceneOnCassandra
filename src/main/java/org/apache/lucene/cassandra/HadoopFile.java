package org.apache.lucene.cassandra;

/**
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

//import java.util.ArrayList;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.lucene.store.CassandraDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class HadoopFile implements File, Serializable {

    private static final long serialVersionUID = 1l;

    // java.io.File thePrivateFile;
    // CassandraFile thePrivateFile;
    FileSystem thePrivateFile;

    Path path;

    /*
     * 
     * [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats | cut -c 21-
     * | sort | uniq -c | sort -n 2 File [TRACE] File(String canonicalPath 2
     * File [TRACE] getCanonicalPath() 2 File [TRACE] list() 4 File [TRACE]
     * isDirectory() 5 RandomAccessFile [TRACE] getFDsync() 14 RandomAccessFile
     * [TRACE] read() 14 RandomAccessFile [TRACE] seek() 14 RandomAccessFile
     * [TRACE] write() 15 File [TRACE] getPath 15 RandomAccessFile [TRACE]
     * length() 16 File [TRACE] delete() 19 RandomAccessFile [TRACE] close() 21
     * File [TRACE] length() 35 File [TRACE] exists() 35 RandomAccessFile
     * [TRACE] RandomAccessFile(File path, String string) 85 File [TRACE]
     * File(File dir, String file) 120 File [TRACE] java.io.File getFile()
     * 
     * [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.write.bigger
     * | cut -c 21- | sort | uniq -c | sort -n 2 File [TRACE] File(String
     * canonicalPath 2 File [TRACE] getCanonicalPath() 2 File [TRACE] list() 4
     * File [TRACE] isDirectory() 5 RandomAccessFile [TRACE] getFDsync() 14
     * RandomAccessFile [TRACE] read() 14 RandomAccessFile [TRACE] seek() 14
     * RandomAccessFile [TRACE] write() 15 File [TRACE] getPath 15
     * RandomAccessFile [TRACE] length() 16 File [TRACE] delete() 19
     * RandomAccessFile [TRACE] close() 21 File [TRACE] length() 35 File [TRACE]
     * exists() 35 RandomAccessFile [TRACE] RandomAccessFile(File path, String
     * string) 85 File [TRACE] File(File dir, String file) 120 File [TRACE]
     * java.io.File getFile()
     * 
     * 
     * [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.read | cut -c
     * 21- | sort | uniq -c | sort -n 1 File [TRACE] list() 1 uments 2 File
     * [TRACE] File(String canonicalPath 2 File [TRACE] exists() 2 File [TRACE]
     * getCanonicalPath() 2 File [TRACE] isDirectory() 3 7 File [TRACE]
     * File(File dir, String file) 7 RandomAccessFile [TRACE]
     * RandomAccessFile(File path, String string) 7 RandomAccessFile [TRACE]
     * close() 7 RandomAccessFile [TRACE] length() 14 File [TRACE] java.io.File
     * getFile() 16 File [TRACE] getPath 22 RandomAccessFile [TRACE] read() 22
     * RandomAccessFile [TRACE] seek()
     * 
     * 
     * [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.read.bigger |
     * cut -c 21- | sort | uniq -c | sort -n 1 File [TRACE] list() 1 uments 2
     * File [TRACE] File(String canonicalPath 2 File [TRACE] exists() 2 File
     * [TRACE] getCanonicalPath() 2 File [TRACE] isDirectory() 5 7 File [TRACE]
     * File(File dir, String file) 7 RandomAccessFile [TRACE]
     * RandomAccessFile(File path, String string) 7 RandomAccessFile [TRACE]
     * close() 7 RandomAccessFile [TRACE] length() 14 File [TRACE] java.io.File
     * getFile() 16 File [TRACE] getPath 24 RandomAccessFile [TRACE] read() 24
     * RandomAccessFile [TRACE] seek()
     */
    private static Logger logger = LoggerFactory.getLogger(File.class);

    final static String outputFile = "TestColumnGroupOpen";

    final private static Configuration conf = new Configuration();

    // TODO create 0 bytes file in the cassandra.
    public HadoopFile(String canonicalPath) {
        // canonicalPath = "index1/thisfile";
        // TODO Auto-generated constructor stub
        logger.trace("File(String canonicalPath: {})", canonicalPath);

        // set default file system to local file system
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        // must set a conf here to the underlying FS, or it barks
        RawLocalFileSystem rawLFS = new RawLocalFileSystem();
        rawLFS.setConf(conf);
        thePrivateFile = new LocalFileSystem(rawLFS);
        thePrivateFile.setVerifyChecksum(false);

        path = new Path(thePrivateFile.getWorkingDirectory(), canonicalPath);

        long length;
        try {
            length = thePrivateFile.getFileStatus(path).getLen();
            logger.info("length() {}", length);
            System.out.println("output file: " + path);
            System.out.println("exist: " + thePrivateFile.exists(path));
            logger.trace("thePrivateFile.getFileStatus {}",
                    thePrivateFile.getFileStatus(path));

            if (!thePrivateFile.exists(path)) {
                thePrivateFile.create(path, true);
            } else {
                // thePrivateFile.create(path, true);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        System.out.println("output file: " + path);
        System.out.println("output file: "
                + thePrivateFile.getWorkingDirectory());

    }

    public HadoopFile(File dir, String file) { // TODO create file in a directory.
        logger.trace("File(File dir  {}, String file {})", dir.getPath(), file);

        // set default file system to local file system
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        // must set a conf here to the underlying FS, or it barks
        RawLocalFileSystem rawLFS = new RawLocalFileSystem();
        rawLFS.setConf(conf);
        thePrivateFile = new LocalFileSystem(rawLFS);
        thePrivateFile.setVerifyChecksum(false);
        path = new Path(dir.getPath() + "/" + file);
        try {
            if (!thePrivateFile.exists(path)) {
                thePrivateFile.create(path, true);
            } else {
                // thePrivateFile.create(path, true);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.exists();

    }

    // public java.io.File getFile() { // TODO not relaly imoprtant.
    // logger.info("java.io.File getFile()");
    // return thePrivateFile;
    // }

    // public CassandraFile getFile() { // TODO not relaly imoprtant.
    // logger.info("java.io.File getFile()");
    // return thePrivateFile;
    // }

    public boolean exists() { // TODO check if the file is
                              // exists.
                              // return true;
        boolean exists = false;
        try {
            exists = thePrivateFile.exists(path);
            logger.info("exists() exists: {}, path: {}", exists, path);
            return exists;

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return exists;

    }

    public boolean isDirectory() { // TODO always return
                                                      // true.
        try {
            logger.info("isDirectory() {}", thePrivateFile.isDirectory(path));
            return thePrivateFile.isDirectory(path);// .isDirectory();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public String[] list(java.io.FilenameFilter filenameFilter) { // TODO return
        // a list of
        // files.
        logger.trace("list(filenameFilter: {})", filenameFilter);

        List<String> theList = new ArrayList<String>();// =
                                                       // thePrivateFile.listFiles(null,
                                                       // false)

        RemoteIterator<LocatedFileStatus> ritr = null;
        try {
            ritr = thePrivateFile.listFiles(path, true);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (ritr.hasNext()) {
                // returns the filename or directory name if directory
                Path p = ritr.next().getPath();
                String name = p.getName();
                theList.add(name);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String[] theArray = theList.toArray(new String[theList.size()]);
        Arrays.sort(theArray);
        logger.info("list() {}", Arrays.toString(theArray));
        return theArray;
    }

    public String getCanonicalPath() throws IOException { // TODO get the path.
        logger.info("getCanonicalPath() {}", path.toString());
        return getPath();
    }

    public long length() { // TODO length of the file.
        long length = 0;
        try {
            length = thePrivateFile.getFileStatus(path).getLen();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.info("length() {}", length);
        return length;
    }

    public boolean delete() { // TODO delete the file.
        logger.info("delete()");
        try {
            return thePrivateFile.delete(path, false);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    public boolean mkdirs() { // TODO just return true.
        logger.info("mkdirs()");
        try {
            return thePrivateFile.mkdirs(path);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;
    }

    // TODO pointer to the right file. like a lot of indices
    public String getPath() {
        try {
            logger.trace("getPath() thePrivateFile {}",
                    thePrivateFile.getFileStatus(path));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            logger.trace("path {}", path);
            logger.trace("thePrivateFile.getFileStatus {}",
                    thePrivateFile.getFileStatus(path));
            logger.trace("getPath() {}", thePrivateFile.getFileStatus(path)
                    .getPath().toString().replaceAll("file:", ""));
            return thePrivateFile.getFileStatus(path).getPath().toString()
                    .replaceAll("file:", "");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    // TODO
    // pointer to the right file. like a lot of indices
    public long lastModified() { // TODO set last modified.
        logger.info("lastModified()");

        FileStatus[] status;
        try {
            status = thePrivateFile.listStatus(path);

            for (FileStatus file : status) {
                return file.getModificationTime();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return 0;
    }

    public boolean createNewFile() throws IOException {
        logger.info("createNewFile()"); //
        // TODO pointer to the right file. like a lot of indices
        return thePrivateFile.createNewFile(path); // TODO create empty file.
    }

    // TODO pointer to the right file. like a lot of indices
    public String getAbsolutePath() {
        try {
            logger.trace("getAbsolutePath() {}", getCanonicalPath());
            return getCanonicalPath(); // TODO get the path.
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public File get(File directory, String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public File get(String canonicalPath) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RandomAccessFile getRandomAccessFile(File fullFile,
            String permissions) throws FileNotFoundException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public java.nio.file.Path toPath() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean renameTo(File file) {
        // TODO Auto-generated method stub
        return false;
    }
    
    public long getTotalSpace() {
        logger.info("getTotalSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public long getUsableSpace() {
        logger.info("getUsableSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public long getFreeSpace() {
        logger.info("getFreeSpace() IS CALLED!!! ");
        return Long.MAX_VALUE;
    }
    
    public File[] listFiles() {
        // TODO Implement this.
        return null;
    }
    
    public File[] listFiles(CassandraFileFilter filter) {
        // TODO Implement this.
        return null;
    }
    
    public String getName() {
        return thePrivateFile.getName();
    }
    
    public String getParent(boolean dummy) {
        // TODO Implement this.
        return null;
    }
    
    public File getParentFile() {
        // TODO Implement this.
        return null;
    }
    
    public boolean canRead() {
     // TODO Implement this.
        return false;
    }
    
    public boolean isInvalid() {
        // TODO Implement this.
        return false;
    }
    
    public FileDescriptor getFileDescriptor() {
        // TODO
        return null;
    }
    
    public void write(int b, boolean append)  throws IOException {
        // TODO
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        // TODO
    }

    /**
     * TODO 
     * {@link org.apache.lucene.cassandra.File#read() read()}
     */
    @Override
    public int read() {
        return -1;
    }
    

}
