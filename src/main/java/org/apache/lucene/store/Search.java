package org.apache.lucene.store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// http://wiki.apache.org/lucene-java/ImproveSearchingSpeed
public class Search {

    private static Logger logger = LoggerFactory.getLogger(Search.class);

    String field = "contents";

    String queries = "California";

    int hitsPerPage = 40;

    boolean raw = false;

    public static boolean useCassandraStorage = false;

    public static boolean useACassandra = true;
    
    String cassandraDirectory = null;
    
    Directory dir = null;
    IndexReader reader = null;
    Analyzer analyzer;
    IndexSearcher searcher;

    public static void main(String[] args) {
        new Search(args);
    }
    
    public Search(String keyspace, String columnFamily, int blockSize, String indexPath) {

        try {
            logger.trace("initializing start");

            dir =
                    org.apache.lucene.cassandra.CassandraDirectory.open(
                            new org.apache.lucene.cassandra.CassandraFile(cassandraDirectory,
                                    indexPath, IOContext.READ, true, keyspace,
                                    columnFamily, blockSize), IOContext.READ,
                            null, keyspace, columnFamily, blockSize, blockSize);

            reader = DirectoryReader.open(dir);
            searcher = new IndexSearcher(reader);
            analyzer = new StandardAnalyzer(Version.LUCENE_46);

        } catch (IOException e) {
            logger.error("ioexception encountered ", e);
        } catch (Exception e) {
            logger.error("Exception encountered ", e);
        }
    }
    
    public int searchOn(String queries) {
        logger.trace("search start");

        try {
            QueryParser parser =
                    new QueryParser(Version.LUCENE_46, field, analyzer);
            
            Query  query = parser.parse(queries);
            
            logger.trace("searching for: " + query.toString(field));
            
            Date start = new Date();
            searcher.search(query, null, 100);
            Date end = new Date();
            logger.trace("Time: " + (end.getTime() - start.getTime()) + "ms");
            
            TopDocs results = searcher.search(query, 5 * hitsPerPage);
            //ScoreDoc[] hits = results.scoreDocs;

            int numTotalHits = results.totalHits;
            
            return numTotalHits;
        } catch (ParseException e) {
            logger.error("1 search fail on " + queries, e);
        } catch (IOException e) {
            logger.error("2 search fail on " + queries, e);
        }  catch (Exception e) {
            logger.error("3 search fail on " + queries, e);
        }
        
        return -1;
    }
    
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            logger.error("unable to close ", e);
            e.printStackTrace();
        }
    }

    public Search(String[] args) {
        try {
            System.out.println("search start");
            String indexPath = "index0";
            String keyspace = "lucene0";
            String columnFamily = "index0";
            int blockSize = 16384;
            
            for (int i = 0; i < args.length; i++) {
                if ("-field".equals(args[i])) {
                    field = args[i + 1];
                    i++;
                } else if ("-queries".equals(args[i])) {
                    queries = args[i + 1];
                    i++;
                } else if ("-raw".equals(args[i])) {
                    raw = true;
                } else if ("-native".equals(args[i])) {
                    useCassandraStorage = false;
                } else if ("-cassandra-dir".equals(args[i])) {
                    cassandraDirectory = args[i + 1];
                    i++;
                }
            }


            if (useCassandraStorage) {
                dir =
                        org.apache.lucene.cassandra.CassandraDirectory.open(
                                new org.apache.lucene.cassandra.CassandraFile(
                                        cassandraDirectory, indexPath, IOContext.READ, true,
                                        keyspace, columnFamily, blockSize),
                                IOContext.READ, null, keyspace, columnFamily,
                                blockSize, blockSize);
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
            }

            reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);

            QueryParser parser =
                    new QueryParser(Version.LUCENE_46, field, analyzer);

            BufferedReader in =
                    new BufferedReader(
                            new InputStreamReader(System.in, "UTF-8"));

            Query query = parser.parse(queries);
            System.out.println("searching for: " + query.toString(field));

            Date start = new Date();
            searcher.search(query, null, 100);
            Date end = new Date();
            System.out.println("Time: " + (end.getTime() - start.getTime())
                    + "ms");

            doPagingSearch(in, searcher, query, hitsPerPage, raw,
                    queries == null);

            reader.close();
        } catch (IOException e) {
            logger.error("ioexception encountered ", e);
        } catch (ParseException e) {
            logger.error("ParseException encountered ", e);
        } catch (Exception e) {
            logger.error("Exception encountered ", e);
        }

    }

    public static void doPagingSearch(BufferedReader in,
            IndexSearcher searcher, Query query, int hitsPerPage, boolean raw,
            boolean interactive) throws IOException {

        // Collect enough docs to show 5 pages
        TopDocs results = searcher.search(query, 5 * hitsPerPage);
        ScoreDoc[] hits = results.scoreDocs;

        int numTotalHits = results.totalHits;
        System.out.println(numTotalHits + " total matching documents");

        int start = 0;
        int end = Math.min(numTotalHits, hitsPerPage);

        while (true) {
            if (end > hits.length) {
                System.out
                        .println("Only results 1 - " + hits.length + " of "
                                + numTotalHits
                                + " total matching documents collected.");
                System.out.println("Collect more (y/n) ?");
                String line = in.readLine();
                if (line.length() == 0 || line.charAt(0) == 'n') {
                    break;
                }

                hits = searcher.search(query, numTotalHits).scoreDocs;
            }

            end = Math.min(hits.length, start + hitsPerPage);

            for (int i = start; i < end; i++) {
                if (raw) { // output raw format
                    System.out.println("doc=" + hits[i].doc + " score="
                            + hits[i].score);
                    continue;
                }

                Document doc = searcher.doc(hits[i].doc);
                String path = doc.get("path");
                if (path != null) {
                    System.out.println((i + 1) + ". " + path);
                    String title = doc.get("title");
                    if (title != null) {
                        System.out.println("   Title: " + doc.get("title"));
                    }
                } else {
                    System.out.println((i + 1) + ". "
                            + "No path for this document");
                }

            }

            if (!interactive || end == 0) {
                break;
            }

            if (numTotalHits >= end) {
                boolean quit = false;
                while (true) {
                    System.out.print("Press ");
                    if (start - hitsPerPage >= 0) {
                        System.out.print("(p)revious page, ");
                    }
                    if (start + hitsPerPage < numTotalHits) {
                        System.out.print("(n)ext page, ");
                    }
                    System.out
                            .println("(q)uit or enter number to jump to a page.");

                    String line = in.readLine();
                    if (line.length() == 0 || line.charAt(0) == 'q') {
                        quit = true;
                        break;
                    }
                    if (line.charAt(0) == 'p') {
                        start = Math.max(0, start - hitsPerPage);
                        break;
                    } else if (line.charAt(0) == 'n') {
                        if (start + hitsPerPage < numTotalHits) {
                            start += hitsPerPage;
                        }
                        break;
                    } else {
                        int page = Integer.parseInt(line);
                        if ((page - 1) * hitsPerPage < numTotalHits) {
                            start = (page - 1) * hitsPerPage;
                            break;
                        } else {
                            System.out.println("No such page");
                        }
                    }
                }
                if (quit)
                    break;
                end = Math.min(numTotalHits, start + hitsPerPage);
            }
        }
    }
}
