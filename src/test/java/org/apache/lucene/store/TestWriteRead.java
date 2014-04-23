package org.apache.lucene.store;

import java.io.IOException;

import junit.framework.TestCase;

// TODO, rewrite this test case to reference CassandraDirectory in org.apache.lucene.cassandra
public class TestWriteRead extends TestCase {

    CassandraDirectory cassandraDirectory;

    public void setUp() throws IOException {
        cassandraDirectory =
                new CassandraDirectory("lucene1", "index1", 10, 10);
    }

    public void tearDown() throws IOException {
        if (cassandraDirectory != null) {
            cassandraDirectory.close();
        }
    }

    public void testWriteFile() throws IOException {
        IndexOutput indexOutput =
                cassandraDirectory.createOutput("hello.txt", IOContext.DEFAULT);
        indexOutput.writeString("hello sun");
        indexOutput.writeString("hello mercury");
        indexOutput.flush();

        indexOutput =
                cassandraDirectory.createOutput("hihi.txt", IOContext.DEFAULT);
        indexOutput.writeString("hihi world");
        indexOutput.flush();
        
        indexOutput = 
                cassandraDirectory.createOutput("segments_1", IOContext.DEFAULT);
        indexOutput.writeString("hihi world");
        indexOutput.flush();
    }

    public void testReadFile() throws IOException {
        IndexInput indexInput =
                cassandraDirectory.openInput("hello.txt", IOContext.DEFAULT);
        String[] values = cassandraDirectory.listAll();
        String fileContent = indexInput.readString();
        System.out.println("test1 => " + fileContent);

        indexInput =
                cassandraDirectory.openInput("hihi.txt", IOContext.DEFAULT);
        fileContent = indexInput.readString();
        System.out.println("test2 => " + fileContent);
    }

    public void testUpdate() {

    }

    public void testSearch() {

    }
    
    public void testReadStrings() throws IOException {
        IndexInput indexInput =
                cassandraDirectory.openInput("segments_1", IOContext.DEFAULT);
        String actualDataPoint = indexInput.readString();
        System.out.println("actualDataPoint '" + actualDataPoint +"'");  
        
    }

}
