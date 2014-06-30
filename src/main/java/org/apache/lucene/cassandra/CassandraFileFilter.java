package org.apache.lucene.cassandra;

/**
 * A filter for abstract pathnames.
 * 
 * <p>
 * Instances of this interface may be passed to the <code>{@link
 * File#listFiles(org.apache.lucene.cassandra.CassandraFileFilter) listFiles(CassandraFileFilter)}</code>
 * method of the <code>{@link org.apache.lucene.cassandra.File}</code> class.
 * 
 */
public interface CassandraFileFilter {

    /**
     * Tests whether or not the specified abstract pathname should be included
     * in a pathname list.
     * 
     * @param pathname
     *            The abstract pathname to be tested
     * @return <code>true</code> if and only if <code>pathname</code> should be
     *         included
     */
    boolean accept(File pathname);
}
