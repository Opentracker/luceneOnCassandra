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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.util.ArrayList;
import java.io.Serializable;
import java.io.SyncFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class FSRandomAccessFile implements RandomAccessFile, Serializable {
    // CassandraRandomAccessFile thePrivateFile;
    java.io.RandomAccessFile thePrivateFile;

    // File currentFile = null;

    private static Logger logger = LoggerFactory
            .getLogger(RandomAccessFile.class);

    /**
     * 
     */
    private static final long serialVersionUID = -526415350772290568L;
    private String path;

    public FSRandomAccessFile(File path, String permission)
            throws FileNotFoundException {
        try {
            logger.info("RandomAccessFile(File path {}, permission {} )", path.getCanonicalPath(), permission);
        } catch (IOException e) {
            logger.error("", e);
            e.printStackTrace();
        }
        // currentFile = path;
        // thePrivateFile = new CassandraRandomAccessFile(path.getFile(),
        // string) ;
        try {
            try {
                this.path = path.getCanonicalPath();
                path.createNewFile();
            } catch (IOException e) {
                logger.error("", e);
                e.printStackTrace();
            }

            thePrivateFile =
                    new java.io.RandomAccessFile(path.getCanonicalPath(),
                            permission);
        } catch (IOException e) {
            logger.error("", e);
            e.printStackTrace();
        }
    }

    public long length() throws IOException {
        logger.info("length() {}", thePrivateFile.length());
        return thePrivateFile.length();
    }

    public void close() throws IOException {
        logger.info("close() {}", path);
        thePrivateFile.close();
    }

    public static int writeCount = 0;
    public static long writeTime = 0;
    public void write(byte[] b, int offset, int n) throws IOException { //
        // Writes n bytes from the specified byte array starting at offset off
        // to this fille
        logger.info("write() {}", Util.bytesToHex(b));
        logger.info("write() {}", offset);
        logger.info("write() {}", n);
        long ms = System.currentTimeMillis();
        writeCount++;
        thePrivateFile.write(b, offset, n);
        writeTime += System.currentTimeMillis() - ms;

    }

    public void seek(long pos) throws IOException {
        // Sets the file-pointer offset, measured from the beginning of this
        // file, at which the next read or write occurs. The offset may be set
        // beyond the end of the file.
        // Setting the offset beyond the end of the file does not change the
        // file length. The file length will change only by writing after the
        // offset has been set beyond the end of the file.
        logger.info("seek() {}", pos);
        thePrivateFile.seek(pos);

    }

    public void setLength(long length) throws IOException { // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#setLength%28long%29
        logger.info("setLength() {}", length);
        thePrivateFile.setLength(length);

    }

    public int read(byte[] b, int i, int toRead) throws IOException { // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#read%28byte[],%20int,%20int%29
        // logger.info("read() {}", currentFile.getFile().getName());
        int read = thePrivateFile.read(b, i, toRead);
        logger.info("read int {}", read);
        logger.info("Util.bytesToHex({})", Util.bytesToHex(b));

        return read;
    }

    public File getFile() {
        logger.info("getFile()");
        return null;
    }

    public void getFDsync() throws SyncFailedException, IOException {
        logger.info("getFDsync()");
        thePrivateFile.getFD().sync(); // write into cassandra. //
                                       // http://docs.oracle.com/javase/7/docs/api/java/io/FileDescriptor.html#sync()
        // thePrivateFile.getFDsync();
    }

    public boolean getFDvalid() throws IOException {
        logger.info("getFDvalid()"); // test if this file
        return thePrivateFile.getFD().valid();
        // return thePrivateFile.getFDvalid();
    }

}
