package org.apache.lucene.store;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.cassandra.ACassandraFile;
import org.apache.lucene.cassandra.ACassandraRandomAccessFile;
import org.apache.lucene.cassandra.FSFile;
import org.apache.lucene.cassandra.FSRandomAccessFile;
import org.apache.lucene.cassandra.OpentrackerInfoStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leadboxer.util.LuceneSettings;

import proj.zoie.api.impl.ZoieMergePolicy;

// http://wiki.apache.org/lucene-java/ImproveIndexingSpeed
//http://search-lucene.blogspot.nl/2008/08/indexing-speed-factors.html,mergeFactor, X*mergeFactor= maxMergeDocs, 
public class IndexFiles {

    public static boolean useCassandraStorage = true;

    public static boolean isCommitTimeBased = true;

    public static boolean useACassandra = true;

    public static boolean useNRT = false;

    public static boolean useInfoStream = false;

    private static Logger logger = LoggerFactory.getLogger(IndexFiles.class);

    private static long counter = 0;

    private static long limit = 10781;
    
    public IndexFiles() {
        
    }
    
    public static MergePolicy getPolicy(String policy, double ratio) {
        MergePolicy usePolicy = null;
        switch (policy) {
        case "zoie":
            //600k docs in 1239s (484 docs/s)
            ZoieMergePolicy zmp = new ZoieMergePolicy();
            zmp.setNumLargeSegments(100000);
            zmp.setMaxSmallSegments(10000);
            usePolicy = zmp;
            // [root@gl04 lucenceOnCassandra]# sh search.sh
            // search start
            // searching for: hello
            // Time: 350ms
            // 568 total matching documents
            break;
        case "logbyte":
            // 600korg.apache.lucene.index.
            LogByteSizeMergePolicy opentrackerTieredMergePolicy = new LogByteSizeMergePolicy();
            opentrackerTieredMergePolicy.setMaxMergeMB(10);
            opentrackerTieredMergePolicy.setMaxMergeMBForForcedMerge(10);
            usePolicy = opentrackerTieredMergePolicy;
            break;
        case "none":
            MergePolicy none = NoMergePolicy.NO_COMPOUND_FILES;//
            usePolicy = none;
        default:
            // 600k docs in 1490s (402 docs/s)
            TieredMergePolicy tmp = new TieredMergePolicy();
            // [root@gl04 lucenceOnCassandra]# sh search.sh
            // search start
            // Time: 1088ms
            // 568 total matching documents
            tmp.setMaxMergeAtOnce(5); // default 10
            tmp.setSegmentsPerTier(100); // default 10
            tmp.setForceMergeDeletesPctAllowed(30.0); // default 10.0
            tmp.setNoCFSRatio(ratio);
            //opentrackerTieredMergePolicy.setMaxCFSSegmentSizeMB(10);
            //opentrackerTieredMergePolicy.setMaxMergedSegmentMB(10);
            //opentrackerTieredMergePolicy.setMinMergeMB(8);
            usePolicy = tmp;
        }
        
        return usePolicy;
    }

    public static void main(String[] args) {

        String usage =
                "java org.apache.lucene.demo.IndexFiles"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles";
        // the directory where the index is stored.
        String indexPath = "index0";
        String keyspace = "lucene0";
        String columnFamily = "index0";
        String cassandraDirectory = null;
        int blockSize = 16384;
        // index the document with the specified docsPath.
        String docsPath = "test";

        // when create is false, it will be create or append which means an
        // update.
        boolean create = true;
        boolean forceMerge = false;
        boolean useCFS = false;

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                i++;
            } else if ("-docs".equals(args[i])) {
                docsPath = args[i + 1];
                i++;
            } else if ("-update".equals(args[i])) {
                create = false;
            } else if ("-merge".equals(args[i])) {
                logger.error("force merge");
                forceMerge = true;
            } else if ("-limit".equals(args[i])) {
                limit = Long.parseLong(args[i + 1]);
            } else if ("-native".equals(args[i])) {
                useCassandraStorage = false;
            } else if ("-cfs".equals(args[i])) {
                useCFS = true;
            } else if ("-cassandra-dir".equals(args[i])) {
                cassandraDirectory = args[i + 1];
                i++;
            } else if ("-keyspace".equals(args[i])) {
                keyspace = args[i + 1];
                i++;
            } else if ("-column-family".equals(args[i])) {
                columnFamily = args[i + 1];
                i++;
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final File docDir = new File(docsPath);
        if (!docDir.exists() || !docDir.canRead()) {
            System.out
                    .println("Document directory '"
                            + docDir.getAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        Directory dir = null;
        NRTCachingDirectory cachedFSDir = null;
        IndexWriter writer = null;

        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            if (useCassandraStorage) {
                // BLOCKSIZE MINIMUM 16384
                dir =
                        org.apache.lucene.cassandra.CassandraDirectory.open(
                                new org.apache.lucene.cassandra.CassandraFile(
                                        cassandraDirectory, indexPath,
                                        IOContext.DEFAULT, true, keyspace,
                                        columnFamily, blockSize),
                                IOContext.DEFAULT, null, keyspace,
                                columnFamily, blockSize, blockSize);
                cachedFSDir = new NRTCachingDirectory(dir, 5.0, 60.0);
            } else {
                if (useACassandra)
                    dir =
                            org.apache.lucene.cassandra.FSDirectory
                                    .open(new org.apache.lucene.cassandra.ACassandraFile(
                                            indexPath));
                else
                    dir =
                            org.apache.lucene.cassandra.FSDirectory
                                    .open(new org.apache.lucene.cassandra.FSFile(
                                            indexPath));

                cachedFSDir = new NRTCachingDirectory(dir, 5.0, 60.0);
            }

            dir.createOutput(indexPath, IOContext.DEFAULT);
            dir.openInput(indexPath, IOContext.DEFAULT);
            Analyzer analyzer = new StandardAnalyzer(LuceneSettings.currentVersion);

            IndexWriterConfig iwc =
                    new IndexWriterConfig(LuceneSettings.currentVersion, analyzer);
            logger.info("merge policy = {}", iwc.getMergePolicy().getClass()
                    .getName());
            logger.info("merge scheduler = {} ", iwc.getMergeScheduler()
                    .getClass().getName());

 
            // when you index a lot of documents, enable the following but also,
            // increase xmx for this jvm.
            iwc.setRAMBufferSizeMB(512.0);
            iwc.setMaxThreadStates(24);
            
            double ratio = -1;
            if (useCFS) {
                ratio = 1;
            } else {
                ratio = 0;
            }
            iwc.setMergePolicy(getPolicy("tiered", ratio));
            // A MergeScheduler that runs each merge using a separate thread.
            iwc.setMergeScheduler(new ConcurrentMergeScheduler());

            // lucene 5.0
            // iwc.setValidateAtMerge(true)

            // Turn off compound file format.
            // Call setUseCompoundFile(false). Building the compound file format
            // takes time during indexing (7-33% in testing for LUCENE-888).
            // However, note that doing this will greatly increase the number of
            // file descriptors used by indexing and by searching, so you could
            // run out of file descriptors if mergeFactor is also large.

            iwc.setUseCompoundFile(useCFS);// false

            // NOTE : turning this on give information about lucene merge
            // details but slow
            // indexing performance.
            if (useInfoStream) {
                iwc.setInfoStream(new OpentrackerInfoStream());
            }

            if (create) {
                // create a new index in the directory, removing any previously
                // indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            if (useNRT) {
                writer = new IndexWriter(cachedFSDir, iwc);
            } else {
                writer = new IndexWriter(dir, iwc);
            }
            indexDocs(writer, docDir, forceMerge);

            // NOTE: if you want to maximize search performance, you can
            // optionally
            // call forceMerge here. This can be a terribly costly operation, so
            // generally
            // it's only worth it when your index is relatively static (ie
            // you're done
            // adding documents to it):
            // System.out.println("forcing merge now.");
            if (writer != null && forceMerge) {
                // writer.forceMerge(50);
                // writer.commit();
            }

            if (writer != null) {
                writer.close();
            }

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime()
                    + " total milliseconds");
            logger.error("write/ ms {}/ {} ms",
                    ACassandraFile.writeCount, ACassandraFile.writeTime);
            logger.error("read/ ms {}/ {} ms",
                    ACassandraRandomAccessFile.readCount, ACassandraRandomAccessFile.readTime);
            logger.error("seek/ ms {}/ {} ms",
                    ACassandraRandomAccessFile.seekCount, ACassandraRandomAccessFile.seekTime);
            logger.error("create/ ms {}/ {} ms",
                    ACassandraFile.createCount, ACassandraFile.createTime);
            logger.error("delete/ ms {}/ {} ms",
                    ACassandraFile.deleteCount, ACassandraFile.deleteTime);
            logger.error("emptyList/ ms {}/ {} ms",
                    ACassandraFile.emptyListCount, ACassandraFile.emptyListTime);
            logger.error("exists/ ms {}/ {} ms",
                    ACassandraFile.existsCount, ACassandraFile.existsTime);
            logger.error("list/ ms {}/ {} ms",
                    ACassandraFile.listCount, ACassandraFile.listTime);
            logger.error("getACF/ ms {}/ {} ms",
                    ACassandraFile.getACFCount, ACassandraFile.getACFTime);
            logger.error("getDNC/ ms {}/ {} ms",
                    ACassandraFile.getDNCount, ACassandraFile.getDNTime);
            logger.error("getRAF/ ms {}/ {} ms",
                    ACassandraFile.getRAFCount, ACassandraFile.getRAFTime);
            logger.error("write file/ ms {}/ {} ms",
                    ACassandraFile.writeCount, ACassandraFile.writeTime);
            logger.error("getDNC file/ ms {}/ {} ms",
                    FSFile.getDNCount, FSFile.getDNTime);
            logger.error("write file/ ms {}/ {} ms",
                    FSRandomAccessFile.writeCount, FSRandomAccessFile.writeTime);

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass()
                    + " \n with message: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (dir != null) {
                try {
                    dir.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is
     * given, recurses over files and directories found under the give
     * directory.
     * 
     * NOTE: This method indexes one document per input file. This is slow. For
     * good throughput, put multiple documents into your input file(s). An
     * example of this is in the benchmark module, which can create "line doc"
     * files, one document per line, using the WriteLineDocTask
     * 
     * @param writer
     *            Writer to the index where the given file/dir info will be
     *            stored
     * @param file
     *            The file to index, or the directory to recurse into to find
     *            files to index
     * @throws IOException
     *             If there is a low-level I/O error
     */
    static long time = System.currentTimeMillis();

    public static void indexDocs(IndexWriter writer, File file,
            boolean forceMerge) {
        // do not try to index files that cannot be read
        try {

            if (file.canRead()) {
                if (counter > limit) {
                    return;
                }
                if (file.isDirectory()) {
                    String[] files = file.list();
                    // an IO error could occur
                    if (files != null) {
                        for (int i = 0; i < files.length; i++) {
                            indexDocs(writer, new File(file, files[i]),
                                    forceMerge);
                        }
                    }

                } else {
                    FileInputStream fis;
                    try {
                        fis = new FileInputStream(file);
                    } catch (FileNotFoundException fnfe) {
                        // at least on windows, some temporary files raise this
                        // exception with
                        // an "access denied" message checking if the file can
                        // be
                        // read doesn't
                        // help.
                        return;
                    }

                    try {

                        // make a new, empty document
                        Document doc = new Document();

                        /*
                         * Add the path of the file as a field named "path". Use
                         * a field that is indexed (i.e. searchable), but don't
                         * tokenize the field into separate words and don't
                         * index term frequency or positional information:
                         */
                        Field pathField =
                                new StringField("path", file.getPath(),
                                        Field.Store.YES);
                        doc.add(pathField);

                        /*
                         * Add the last modified date of the file a field named
                         * "modified". Use a LongField that is indexed (i.e.
                         * efficiently filterable with NumericRangeFilter). You
                         * could instead create a number based on
                         * year/month/day/hour/minutes/seconds, down the
                         * resolution you require. For example the long value
                         * 2014012817 would mean January 29, 2014 5-6 PM.
                         */
                        doc.add(new LongField("modified", file.lastModified(),
                                Field.Store.NO));

                        /*
                         * Add the contents of the file to a field named
                         * "contents". Specify a Reader, so that the text of the
                         * file is tokenized and indexed, but not stored. Note
                         * that FileReader expects the file to be in UTF-8
                         * encoding. If that's not the case searching for
                         * special characters will fail.
                         */
                        doc.add(new TextField("contents", new BufferedReader(
                                new InputStreamReader(fis, "UTF-8"))));

                        if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                            // new index, so we just add the document (no old
                            // document can be
                            // there):
                            System.out.println("adding " + file);
                            writer.addDocument(doc);
                        } else {
                            /*
                             * Existing index (an old copy of this document may
                             * have been indexed) so we use updateDocument
                             * instead to replace the old one matching the exact
                             * path, if present:
                             */
                            System.out.println("updating " + file);
                            writer.updateDocument(
                                    new Term("path", file.getPath()), doc);
                        }
                        counter++;

                    } finally {
                        fis.close();
                    }

                    long diff = System.currentTimeMillis() - time;
                    // forceMerge && counter != 0 && counter % 5000 == 0
                    // && writer.hasUncommittedChanges() ||
                    if ((forceMerge && isCommitTimeBased && diff > 5000 && writer
                            .hasUncommittedChanges())) {
                        writer.prepareCommit();
                        writer.commit();
                        // TODO close it and reopen, maybe write a better class for this new design.
                        // System.gc();
                        time = System.currentTimeMillis();
                        Runtime runtime = Runtime.getRuntime();
                        logger.error("use memory {}/{} byte",
                                (runtime.totalMemory() - runtime.freeMemory()),
                                runtime.totalMemory());
                        logger.error("write/ ms {}/ {} ms",
                                ACassandraFile.writeCount, ACassandraFile.writeTime);
                        logger.error("read/ ms {}/ {} ms",
                                ACassandraRandomAccessFile.readCount, ACassandraRandomAccessFile.readTime);
                        logger.error("seek/ ms {}/ {} ms",
                                ACassandraRandomAccessFile.seekCount, ACassandraRandomAccessFile.seekTime);
                        logger.error("sync/ ms {}/ {} ms",
                                ACassandraRandomAccessFile.syncCount, ACassandraRandomAccessFile.syncTime);

                    }
                }
            }
        } catch (OutOfMemoryError e) {
            // TODO maybe do error handling here. like notify monitoring system
            // / sms
            // log current memory into the system.
            logger.error("running out of memory!!! increase memory ", e);
            Runtime runtime = Runtime.getRuntime();
            logger.error("use memory {}/{} byte",
                    (runtime.totalMemory() - runtime.freeMemory()),
                    runtime.totalMemory());
            return;
        } catch (IOException e) {
            logger.error("unable to index file " + file, e);
            // TODO, maybe do error handling here. like notify monitoring system
            // / sms
            // and also put into queue and redo it again later.
        } catch (Error e) {
            logger.error("error caught ", e);
        }
    }
}
