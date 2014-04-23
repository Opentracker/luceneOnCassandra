package org.apache.lucene.cassandra.trash;

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

import org.apache.lucene.cassandra.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @lucene.internal */
public class CassandraRandomAccessFile implements Serializable {
    java.io.RandomAccessFile thePrivateFile;

    private static Logger logger = LoggerFactory
            .getLogger(CassandraRandomAccessFile.class);

    /**
     * 
     */
    private static final long serialVersionUID = -526415350772290568L;

    public CassandraRandomAccessFile(File path, String string)
            throws FileNotFoundException {
        logger.trace("RandomAccessFile(File path, String string)");
        //thePrivateFile = new java.io.RandomAccessFile(null, string);
    }

    public long length() throws IOException {
        logger.trace("length(): " + thePrivateFile.length());
        return thePrivateFile.length();
    }

    public void close() throws IOException {
        logger.trace("close()");
        thePrivateFile.close();
    }

    public void write(byte[] b, int offset, int toWrite) throws IOException {
        logger.trace("write(byte[] b, int offset, int toWrite): " + b);
        logger.trace("write(byte[] b, int offset, int toWrite): " + offset);
        logger.trace("write(byte[] b, int offset, int toWrite): " + toWrite);
        thePrivateFile.write(b, offset, toWrite);

    }

    public void seek(long pos) throws IOException {
        logger.trace("seek(pos): " + pos);
        thePrivateFile.seek(pos);

    }

    public void setLength(long length) throws IOException {
        logger.trace("setLength(length): " + length);
        thePrivateFile.setLength(length);

    }

    public int read(byte[] b, int i, int toRead) throws IOException {
        logger.trace("read(byte[] b, int i, int toRead): " + b);
        logger.trace("read(byte[] b, int i, int toRead): " + i);
        logger.trace("read(byte[] b, int i, int toRead): " + toRead);
        return thePrivateFile.read(b, i, toRead);
    }

    public Closeable getFile() {
        logger.trace("getFile()");
        return thePrivateFile;
    }

    public void getFDsync() throws SyncFailedException, IOException {
        logger.trace("getFDsync()");
        thePrivateFile.getFD().sync();

    }

    public boolean getFDvalid() throws IOException {
        logger.trace("getFDvalid(), ", thePrivateFile.getFD().valid());
        return thePrivateFile.getFD().valid();
    }

}
