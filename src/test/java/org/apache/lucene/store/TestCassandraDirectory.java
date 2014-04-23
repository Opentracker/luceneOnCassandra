package org.apache.lucene.store;

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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

//TODO, rewrite this test case to reference CassandraDirectory in org.apache.lucene.cassandra
public class TestCassandraDirectory extends TestCase {

    CassandraDirectory cassandraDirectory;

    public void setUp() throws IOException {
        cassandraDirectory =
                new CassandraDirectory("lucene1", "index1", 10, 10);
        for (String fileName : cassandraDirectory.listAll()) {
            // System.out.println("deleting.. " + fileName);
            cassandraDirectory.deleteFile(fileName);
        }
        cassandraDirectory.createOutput("sampleFile", IOContext.DEFAULT);
    }

    public void tearDown() throws IOException {
        if (cassandraDirectory != null) {
            cassandraDirectory.close();
        }
    }

    /*
     * public void testOpentracker() throws IOException {
     * cassandraDirectory.createOutput(
     * "www.employeenetwork.com||entryUrl|platform||sessionId||2014-01-15||Windows|https://www.employeenetwork.com/phoenixsuns.html"
     * , IOContext.DEFAULT); }
     */

    public void testListAll() throws IOException {
        String[] files = cassandraDirectory.listAll();
        for (String file : files) {
            System.out.println("listing file => " + file);
        }
        assertEquals(1, files.length);
        assertEquals("sampleFile", files[0]);
    }

    public void testFileExists() throws IOException {
        try {
            assertTrue("The sample file should've been found, but it wasn't.",
                    cassandraDirectory.fileExists("sampleFile"));
            cassandraDirectory.fileExists("dummyFile");
            assertTrue("The dummy file should not've been found, but it was.",
                    true);
        } catch (IOException e) {
        }
    }

    public void testFileModified() throws IOException {
        long fileModified = cassandraDirectory.fileModified("sampleFile");
        System.out.printf("file %s modified on %s", "sampleFile", fileModified);
        long currentTime = new Date().getTime();
        long secondsSinceFileModified = (currentTime - fileModified) / 1000;
        assertTrue(
                "The sample file should have been modified just now, but it wasn't.",
                secondsSinceFileModified >= 0 && secondsSinceFileModified < 1);
    }

    public void testTouchFile() throws IOException, InterruptedException {
        long fileModified = cassandraDirectory.fileModified("sampleFile");
        System.out.println("before " + fileModified);
        TimeUnit.SECONDS.sleep(3);
        cassandraDirectory.touchFile("sampleFile");
        long fileTouched = cassandraDirectory.fileModified("sampleFile");
        System.out.println("after " + fileTouched);
        long currentTime = new Date().getTime();
        long secondsFileUnmodified = (fileTouched - fileModified) / 1000;
        assertTrue(
                "The sample file was not quiet for 3 seconds before it was touched.",
                secondsFileUnmodified >= 2 && secondsFileUnmodified <= 3);
        long secondsSinceFileTouched = (currentTime - fileTouched) / 1000;
        assertTrue(
                "The sample file should'be been touched just now, but it wasn't.",
                secondsSinceFileTouched >= 0 && secondsSinceFileTouched < 1);

    }

    public void testDeleteFile() throws IOException {
        cassandraDirectory.deleteFile("sampleFile");
        assertTrue("The sample file should not've been found, but it was.",
                !cassandraDirectory.fileExists("sampleFile"));
    }

    public void testFileLength() throws IOException {
        assertEquals("The sample file's length should be zero.",
                cassandraDirectory.fileLength("sampleFile"), 0);
    }

    public void testCreateFile() throws IOException {
        for (int fileIndex = 0; fileIndex < 10; fileIndex++) {
            String testFileName = "testFile" + fileIndex;
            cassandraDirectory.createOutput(testFileName, IOContext.DEFAULT);
            cassandraDirectory.fileExists(testFileName);
            assertEquals("The test file's length should be zero.",
                    cassandraDirectory.fileLength(testFileName), 0);
        }
    }

    // chr hex chr hex chr hex chr hex
    // 0 30 3 33 6 36 9 39
    // 1 31 4 34 7 37 A 41
    // 2 32 5 35 8 38 B 42
    //
    // L 4C O 4F C 43 K 4B
    // - 2D D 44 E 45 S 53
    // R 52 I 49 P 50 T 54

    // New Line 0a Vertical Tab 0b
    // End Of EOT
    // Transmission
    //
    // 42 4c 4f 43 4b 2d 30, value=04 30 31 32 33 0a 30 31 32 33
    // B L O C K - 0 EOT 0 1 2 3 NL 0 1 2 3
    //
    // 42 4c 4f 43 4b 2d 31, value=34 35 36 37 38 39 0b
    // B L O C K - 1 4 5 6 7 8 9 VT
    //
    // 42 4c 4f 43 4b 2d 32, value=30 31 32
    // B L O C K - 2 0 1 2
    //
    // 42 4c 4f 43 4b 2d 33, value=33 34 35 36 37 38 39 41
    // B L O C K - 3 3 4 5 6 7 8 9 A
    //
    // 44 45 53 43 52 49 50 54 4f 52, value=7b 22 6c 61 73 74 4d 6f 64 69 66 69
    // 65 64 22 3a 31 33 38 39 31 36 37 35 36 34 31 32 35 2c 22 6e 61 6d 65 22
    // 3a 22 73 61 6d 70 6c 65 46 69 6c 65 22 2c 22 6c 65 6e 67 74 68 22 3a 32
    // 38 2c 22 62 6c 6f 63 6b 73 22 3a 5b 7b 22 63 6f 6c 75 6d 6e 4e 61 6d 65
    // 22 3a 22 42 4c 4f 43 4b 2d 30 22 2c 22 62 6c 6f 63 6b 53 69 7a 65 22 3a
    // 31 30 2c 22 64 61 74 61 4c 65 6e 67 74 68 22 3a 31 30 2c 22 64 61 74 61
    // 4f 66 66 73 65 74 22 3a 30 2c 22 62 6c 6f 63 6b 4e 75 6d 62 65 72 22 3a
    // 30 7d 2c 7b 22 63 6f 6c 75 6d 6e 4e 61 6d 65 22 3a 22 42 4c 4f 43 4b 2d
    // 31 22 2c 22 62 6c 6f 63 6b 53 69 7a 65 22 3a 31 30 2c 22 64 61 74 61 4c
    // 65 6e 67 74 68 22 3a 37 2c 22 64 61 74 61 4f 66 66 73 65 74 22 3a 30 2c
    // 22 62 6c 6f 63 6b 4e 75 6d 62 65 72 22 3a 31 7d 2c 7b 22 63 6f 6c 75 6d
    // 6e 4e 61 6d 65 22 3a 22 42 4c 4f 43 4b 2d 32 22 2c 22 62 6c 6f 63 6b 53
    // 69 7a 65 22 3a 31 30 2c 22 64 61 74 61 4c 65 6e 67 74 68 22 3a 33 2c 22
    // 64 61 74 61 4f 66 66 73 65 74 22 3a 37 2c 22 62 6c 6f 63 6b 4e 75 6d 62
    // 65 72 22 3a 32 7d 2c 7b 22 63 6f 6c 75 6d 6e 4e 61 6d 65 22 3a 22 42 4c
    // 4f 43 4b 2d 33 22 2c 22 62 6c 6f 63 6b 53 69 7a 65 22 3a 31 30 2c 22 64
    // 61 74 61 4c 65 6e 67 74 68 22 3a 38 2c 22 64 61 74 61 4f 66 66 73 65 74
    // 22 3a 30 2c 22 62 6c 6f 63 6b 4e 75 6d 62 65 72 22 3a 33 7d 5d 2c 22 64
    // 65 6c 65 74 65 64 22 3a 66 61 6c 73 65 2c 22 6c 61 73 74 41 63 63 65 73
    // 73 65 64 22 3a 31 33 38 39 31 36 37 35 36 34 31 32 35 7d
    // D E S C R I P T O R {
    // "    l  a    s    t    M  o   d    i    f   i     e    d   " : 1 3 8 9 1
    // 6 7 5 6 4 1 2 5 , "   n   a   m   e   " :
    // "    s    a  m   p   l    e    f   i    l      e   " ,
    // "   l     e   n   g   t     h " : 2 8 , "  b   l    o   c    k   s    " :
    // [ { "   c   o   l    u   m  n   N   a   m  e    " :
    // "   B   L   O  C   K   -    0    " ,
    // "   b   l    o   c   k   S    i    z   e    " : 1 0 ,
    // "     d   a   t    a   L   e   n   g    t    h   " : 1 0 ,
    // "   d    a   t    a   O  f    f    s    e    t   " : 0 ,
    // "   b   l    o  c    k   N   u   m  b   e   r     " : 0 } , {
    // "    c  o   l    u    m  n  N  a    m  e    " :
    // "    B   L   O  C   K   -    1    " ,
    // "   b   l     o  c   k   S    i     z   e   " : 1 0 ,
    // "    d   a   t    a   L   e   n   g    t    h   " : 7 ,
    // "    d   a   t    a   O  f     f    s   e   t    " : 0 ,
    // "    b   l    o   c   k   N   u   m  b   e   r     " : 1 } , {
    // "     c   o   l   u   m  n   N   a   m  e    " :
    // "    B   L  O  C   K   -    2    " ,
    // "    b   l    o   c   k   S    i    z    e   " : 1 0 ,
    // "    d   a   t   a    L   e   n   g   t    h    " : 3 ,
    // "   d   a    t    a   O   f   f     s   e   t    " : 7 ,
    // "    b   l   o    c   k   N   u   m   b  e    r   " : 2 } , {
    // "   c   o   l     u  m   n   N  a   m  e   " :
    // "    B   L  O  C   K    -   3    " ,
    // "    b  l     o   c   k   S   i      z   e   " : 1 0 ,
    // "    d   a    t    a   L   e   n    g   t    h    " : 8 ,
    // "   d    a   t    a   O  f    f     s   e    t   " : 0 ,
    // "    b  l     o   c   k   N  u   m  b    e   r     " : 3 } ] ,
    // "    d  e   l     e   t   e    d   " : f a l s e ,
    // "   l   a   s    t    A   c   c    e    s   s   e    d  " : 1 3 8 9 1 6 7
    // 5 6 4 1 2 5 }
    public void testWriteFile() throws IOException {
        String smallerThanABlock = "0123";
        String exactlyABLock = "0123456789";
        String largerThanABLock = "0123456789A";

        String[] dataSample =
                new String[] { smallerThanABlock, exactlyABLock,
                        largerThanABLock };

        for (String dataPoint : dataSample) {
            writeStrings(new String[] { dataPoint });
        }

        for (String dataPoint : dataSample) {
            List<String> stringsToBeWritten = new ArrayList<String>();
            stringsToBeWritten.addAll(Arrays.asList(dataSample));
            stringsToBeWritten.remove(dataPoint);
            writeStrings(stringsToBeWritten.toArray(new String[] {}));
        }

        PermutationGenerator kPermutation =
                new PermutationGenerator(dataSample.length);
        while (kPermutation.hasMore()) {
            int[] indices = kPermutation.getNext();
            String[] stringsToBeWritten = new String[3];
            for (int i = 0; i < indices.length; i++) {
                System.out.println("indices " + indices[i]);
                stringsToBeWritten[i] = dataSample[indices[i]];
            }
            writeStrings(dataSample);
        }
    }

    public void testReadFile() throws IOException {
        String smallerThanABlock = "0123";
        String exactlyABLock = "0123456789";
        String largerThanABLock = "0123456789A";

        String[] dataSample =
                new String[] { smallerThanABlock, exactlyABLock,
                        largerThanABLock };

        for (String dataPoint : dataSample) {
            readStrings(writeStrings(new String[] { dataPoint }));
        }

        for (String dataPoint : dataSample) {
            List<String> stringsToBeWritten = new ArrayList<String>();
            stringsToBeWritten.addAll(Arrays.asList(dataSample));
            stringsToBeWritten.remove(dataPoint);
            readStrings(writeStrings(stringsToBeWritten
                    .toArray(new String[] {})));
        }

        PermutationGenerator kPermutation =
                new PermutationGenerator(dataSample.length);
        while (kPermutation.hasMore()) {
            int[] indices = kPermutation.getNext();
            String[] stringsToBeWritten = new String[3];
            for (int i = 0; i < indices.length; i++) {
                stringsToBeWritten[i] = dataSample[indices[i]];
            }
            readStrings(writeStrings(dataSample));
        }
    }

    protected String[] writeStrings(String[] dataSample) throws IOException {
        cassandraDirectory.deleteFile("sampleFile");
        IndexOutput indexOutput =
                cassandraDirectory
                        .createOutput("sampleFile", IOContext.DEFAULT);
        int dataLength = 0;
        for (String dataPoint : dataSample) {
            indexOutput.writeString(dataPoint);
            dataLength += dataPoint.length();
        }
        indexOutput.flush();
        System.out.printf("dataSample length %s indexOutput length %s %n",
                dataSample.length, indexOutput.length());
        assertEquals("The index output's current file length is incorrect.",
                dataLength + dataSample.length, indexOutput.length());
        return dataSample;
    }

    protected void readStrings(String[] dataSample) throws IOException {
        IndexInput indexInput =
                cassandraDirectory.openInput("sampleFile", IOContext.DEFAULT);
        int dataLength = 0;
        for (String expectedDataPoint : dataSample) {
            String actualDataPoint = indexInput.readString();
            System.out.println("actualDataPoint " + actualDataPoint);
            assertEquals("The index input's next string did not match.",
                    expectedDataPoint, actualDataPoint);
            dataLength += actualDataPoint.length();
        }
        assertEquals("The index output's current file length is incorrect.",
                dataLength + dataSample.length, indexInput.length());
    }

    public static class PermutationGenerator {

        private int[] a;

        private BigInteger numLeft;

        private BigInteger total;

        // -----------------------------------------------------------
        // Constructor. WARNING: Don't make n too large.
        // Recall that the number of permutations is n!
        // which can be very large, even when n is as small as 20 --
        // 20! = 2,432,902,008,176,640,000 and
        // 21! is too big to fit into a Java long, which is
        // why we use BigInteger instead.
        // ----------------------------------------------------------

        public PermutationGenerator(int n) {
            if (n < 1) {
                throw new IllegalArgumentException("Min 1");
            }
            a = new int[n];
            total = getFactorial(n);
            reset();
        }

        // ------
        // Reset
        // ------

        public void reset() {
            for (int i = 0; i < a.length; i++) {
                a[i] = i;
            }
            numLeft = new BigInteger(total.toString());
        }

        // ------------------------------------------------
        // Return number of permutations not yet generated
        // ------------------------------------------------

        public BigInteger getNumLeft() {
            return numLeft;
        }

        // ------------------------------------
        // Return total number of permutations
        // ------------------------------------

        public BigInteger getTotal() {
            return total;
        }

        // -----------------------------
        // Are there more permutations?
        // -----------------------------

        public boolean hasMore() {
            return numLeft.compareTo(BigInteger.ZERO) == 1;
        }

        // ------------------
        // Compute factorial
        // ------------------

        private static BigInteger getFactorial(int n) {
            BigInteger fact = BigInteger.ONE;
            for (int i = n; i > 1; i--) {
                fact = fact.multiply(new BigInteger(Integer.toString(i)));
            }
            return fact;
        }

        // --------------------------------------------------------
        // Generate next permutation (algorithm from Rosen p. 284)
        // --------------------------------------------------------

        public int[] getNext() {

            if (numLeft.equals(total)) {
                numLeft = numLeft.subtract(BigInteger.ONE);
                return a;
            }

            int temp;

            // Find largest index j with a[j] < a[j+1]

            int j = a.length - 2;
            while (a[j] > a[j + 1]) {
                j--;
            }

            // Find index k such that a[k] is smallest integer
            // greater than a[j] to the right of a[j]

            int k = a.length - 1;
            while (a[j] > a[k]) {
                k--;
            }

            // Interchange a[j] and a[k]

            temp = a[k];
            a[k] = a[j];
            a[j] = temp;

            // Put tail end of permutation after jth position in increasing
            // order

            int r = a.length - 1;
            int s = j + 1;

            while (r > s) {
                temp = a[s];
                a[s] = a[r];
                a[r] = temp;
                r--;
                s++;
            }

            numLeft = numLeft.subtract(BigInteger.ONE);
            return a;

        }

    }

}
