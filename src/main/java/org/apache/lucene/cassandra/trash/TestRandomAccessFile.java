package org.apache.lucene.cassandra.trash;

import java.io.FileNotFoundException;
import java.io.IOException;

public class TestRandomAccessFile {
    public static void main(String[] args) throws IOException {

        String line = null;
        java.io.RandomAccessFile file =
                new java.io.RandomAccessFile("/Users/nejoom/test.test", "rw");
        System.out.println(file.getFilePointer());
        while ((line = file.readLine()) != null) {
            System.out.println(line);
            System.out.println(file.getFilePointer());

//            if (line.contains("Text to be appended with")) {
                file.seek(file.getFilePointer());
                file.write(" new text has been appended".getBytes());
//                break;
//            }
        }
        file.close();
    }
}
