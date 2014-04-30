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
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.util.ArrayList;
import java.io.Serializable;
import java.io.SyncFailedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class HadoopRandomAccessFile implements RandomAccessFile, Serializable {
    FileSystem thePrivateFile;

    Path path;

    FSDataOutputStream out;

    FSDataInputStream in;

    boolean isOpen;


    // java.io.RandomAccessFile thePrivateFile;
    File currentFile = null;

    private int seek;

    private static Logger logger = LoggerFactory
            .getLogger(RandomAccessFile.class);

    /**
     * 
     */
    private static final long serialVersionUID = -526415350772290568L;
    final private static Configuration conf = new Configuration();

    public HadoopRandomAccessFile(File dir, String mode) throws IOException {
        logger.trace("RandomAccessFile(File dir {}, String mode {})",
                dir.getPath(), mode);

        // set default file system to local file system
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        // must set a conf here to the underlying FS, or it barks
        RawLocalFileSystem rawLFS = new RawLocalFileSystem();
        rawLFS.setConf(conf);
        thePrivateFile = new LocalFileSystem(rawLFS);
        thePrivateFile.setVerifyChecksum(false);

        path = new Path(thePrivateFile.getWorkingDirectory(),dir.getPath());
        in = thePrivateFile.open(path);
//        out = thePrivateFile.create(path, true);
        isOpen = true;

    }

    public long length() throws IOException {
        long lenth = thePrivateFile.getFileStatus(path).getLen();

        logger.trace("length() {}", lenth);
        return lenth;
    }

    public void close() throws IOException {
        logger.trace("close()");
        thePrivateFile.close();
        isOpen = false;
    }

    public void write(byte[] b, int offset, int n) throws IOException {
        // Writes n bytes from the specified byte array starting at offset off
        // to this file.
        logger.info("write() {}", Util.bytesToHex(b));
        logger.trace("write() {}", offset);
        logger.trace("write() {}", n);
        out.write(b, offset + seek, n);
    }

    public void seek(long seek) throws IOException {
        // Sets the file-pointer offset, measured from the beginning of this
        // file, at which the next read or write occurs. The offset
        // may be set beyond the end of the file. Setting the offset beyond the
        // end of the file does not change the file length. The file length will
        // change only by writing after the offset has been set beyond the end
        // of the file.
        logger.trace("seek() {}", seek);
        this.seek = (int) seek;

    }

    public void setLength(long length) throws IOException { // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#setLength%28long%29
        logger.info("setLength() {}", length);
        // path.setLength(length);

    }

    public int read(byte[] b, int i, int toRead) throws IOException { // http://docs.oracle.com/javase/7/docs/api/java/io/RandomAccessFile.html#read%28byte[],%20int,%20int%29
        logger.trace("read() {}", path.getName());
        int read = in.read(seek, b, i, toRead);
        logger.trace("pos {}", seek);
        logger.trace("read int {}", seek);
        logger.trace("Util.bytesToHex({})", Util.bytesToHex(b));

        return read;
    }

    // // public CassandraFile getFile() { // TODO dont need
    // public CassandraRandomAccessFile getFile() { // TODO dont need
    // logger.info("getFile()");
    // return thePrivateFile;
    // }

    public void getFDsync() throws SyncFailedException, IOException {
        logger.info("getFDsync()");
        // thePrivateFile.getFD().sync(); // write into cassandra. //
        // http://docs.oracle.com/javase/7/docs/api/java/io/FileDescriptor.html#sync()
        // thePrivateFile.getFDsync();
        out.hsync();
    }

    public boolean getFDvalid() throws IOException {
        logger.info("getFDvalid()"); // test if this file
        // return thePrivateFile.getFD().valid();
        // return thePrivateFile.getFDvalid();
        return isOpen;
    }
}
