package org.apache.lucene.cassandra;

import java.io.IOException;

public class IOUtils {

    public static void closeWhileHandlingException(IOException priorE,
            RandomAccessFile file) throws IOException {
//        org.apache.lucene.util.IOUtils.closeWhileHandlingException(priorE, file.getFile());
        
    }
    
    public static void closeWhileHandlingException(IOException priorE, CassandraRandomAccessFile file) throws IOException {
        
    }

}
