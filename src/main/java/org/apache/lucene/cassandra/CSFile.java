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
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.lucene.store.CassandraDirectory;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class CSFile  implements Serializable {

    private static final long serialVersionUID = 1l;

//    java.io.File thePrivateFile;
    CassandraFile thePrivateFile;
/*

[nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats | cut -c 21- | sort | uniq -c | sort -n
   2 File [TRACE] File(String canonicalPath
   2 File [TRACE] getCanonicalPath()
   2 File [TRACE] list()
   4 File [TRACE] isDirectory()
   5 RandomAccessFile [TRACE] getFDsync()
  14 RandomAccessFile [TRACE] read()
  14 RandomAccessFile [TRACE] seek()
  14 RandomAccessFile [TRACE] write()
  15 File [TRACE] getPath
  15 RandomAccessFile [TRACE] length()
  16 File [TRACE] delete()
  19 RandomAccessFile [TRACE] close()
  21 File [TRACE] length()
  35 File [TRACE] exists()
  35 RandomAccessFile [TRACE] RandomAccessFile(File path, String string)
  85 File [TRACE] File(File dir, String file)
 120 File [TRACE] java.io.File getFile()

 [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.write.bigger | cut -c 21- | sort | uniq -c | sort -n
   2 File [TRACE] File(String canonicalPath
   2 File [TRACE] getCanonicalPath()
   2 File [TRACE] list()
   4 File [TRACE] isDirectory()
   5 RandomAccessFile [TRACE] getFDsync()
  14 RandomAccessFile [TRACE] read()
  14 RandomAccessFile [TRACE] seek()
  14 RandomAccessFile [TRACE] write()
  15 File [TRACE] getPath
  15 RandomAccessFile [TRACE] length()
  16 File [TRACE] delete()
  19 RandomAccessFile [TRACE] close()
  21 File [TRACE] length()
  35 File [TRACE] exists()
  35 RandomAccessFile [TRACE] RandomAccessFile(File path, String string)
  85 File [TRACE] File(File dir, String file)
 120 File [TRACE] java.io.File getFile()

 
 [nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.read | cut -c 21- | sort | uniq -c | sort -n
   1 File [TRACE] list()
   1 uments
   2 File [TRACE] File(String canonicalPath
   2 File [TRACE] exists()
   2 File [TRACE] getCanonicalPath()
   2 File [TRACE] isDirectory()
   3 
   7 File [TRACE] File(File dir, String file)
   7 RandomAccessFile [TRACE] RandomAccessFile(File path, String string)
   7 RandomAccessFile [TRACE] close()
   7 RandomAccessFile [TRACE] length()
  14 File [TRACE] java.io.File getFile()
  16 File [TRACE] getPath
  22 RandomAccessFile [TRACE] read()
  22 RandomAccessFile [TRACE] seek()


[nejoom:~/Documents/workspace/luceneOnCassandra]$ cat stats.read.bigger | cut -c 21- | sort | uniq -c | sort -n
   1 File [TRACE] list()
   1 uments
   2 File [TRACE] File(String canonicalPath
   2 File [TRACE] exists()
   2 File [TRACE] getCanonicalPath()
   2 File [TRACE] isDirectory()
   5 
   7 File [TRACE] File(File dir, String file)
   7 RandomAccessFile [TRACE] RandomAccessFile(File path, String string)
   7 RandomAccessFile [TRACE] close()
   7 RandomAccessFile [TRACE] length()
  14 File [TRACE] java.io.File getFile()
  16 File [TRACE] getPath
  24 RandomAccessFile [TRACE] read()
  24 RandomAccessFile [TRACE] seek()



 */
private static Logger logger = LoggerFactory
.getLogger(File.class);

    // TODO create 0 bytes file in the cassandra.
    public CSFile(String canonicalPath)  {
        // TODO Auto-generated constructor stub
        logger.info("File(String canonicalPath {}", canonicalPath);
//        thePrivateFile = new java.io.File(canonicalPath);
        thePrivateFile = new CassandraFile(null, canonicalPath, IOContext.DEFAULT, true, "lucene0", "index0", 16384);
    }

    public CSFile(File dir, String file) {   // TODO create file in a directory.
        try {
            logger.info("File(File {}, String file {} )", dir.getCanonicalPath(), file);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//      thePrivateFile = new java.io.File(dir.getFile(), file);
      //thePrivateFile = new CassandraFile(dir.getFile(), file);
    }
    

//    public java.io.File getFile() { // TODO not relaly imoprtant.
//        logger.info("java.io.File getFile()");
//        return thePrivateFile;
//    }

    public CassandraFile getFile() { // TODO not relaly imoprtant.
        logger.info("java.io.File getFile()");
        return thePrivateFile;
    }

    public boolean exists() { // TODO check if the file is exists.
        logger.info("exists() {}", thePrivateFile.exists());
        return thePrivateFile.exists();
    }

    public boolean isDirectory() { // TODO always return true.
        logger.info("isDirectory() {}", thePrivateFile.isDirectory());
        return thePrivateFile.isDirectory();
    }

    public String[] list(java.io.FilenameFilter filenameFilter) { // TODO return a list of files.
        String [] theArray = thePrivateFile.list(filenameFilter);
        Arrays.sort(theArray);
        logger.info("list() {}", Arrays.toString(theArray));
        return theArray;
    }

    public String getCanonicalPath() throws IOException{ // TODO get the path.
        logger.info("getCanonicalPath() {}", thePrivateFile.getCanonicalPath());
        return thePrivateFile.getCanonicalPath();
    }

    public long length() {  // TODO length of the file.
        logger.info("length() {}", thePrivateFile.length());
        return thePrivateFile.length();
    }

    public boolean delete() { // TODO delete the file.
//        logger.info("delete() {}", thePrivateFile.delete());
        return thePrivateFile.delete();
    }

    public boolean mkdirs() { // TODO just return true.
//        logger.info("mkdirs() {}", thePrivateFile.mkdirs());
        return thePrivateFile.mkdirs();
    }

    public String getPath() {
        logger.info("getPath() {}", thePrivateFile.getPath());  // TODO pointer to the right file. like a lot of indices
        return thePrivateFile.getPath();
    }


    public long lastModified() {  // TODO set last modified.
        logger.info("lastModified() {}", thePrivateFile.lastModified());  // TODO pointer to the right file. like a lot of indices
        return thePrivateFile.lastModified();
    }

    public boolean createNewFile() throws IOException {
//        logger.info("createNewFile() {}", thePrivateFile.createNewFile());  // TODO pointer to the right file. like a lot of indices
        return thePrivateFile.createNewFile();  // TODO create empty file.
    }

    public String getAbsolutePath() {
        logger.info("getAbsolutePath() {}", thePrivateFile.getAbsolutePath());// TODO pointer to the right file. like a lot of indices
        return thePrivateFile.getAbsolutePath(); // TODO get the path.
    }

}
