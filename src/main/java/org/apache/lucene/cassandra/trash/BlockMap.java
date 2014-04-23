package org.apache.lucene.cassandra.trash;

import java.util.TreeMap;

import org.apache.lucene.store.CassandraDirectory;

/**
 * The <code>BlockMap</code> keeps track of file blocks by their names.
 * Given that the name is a byte array, we rely on a custom comparator that
 * knows how to compare such names.
 */
public class BlockMap extends TreeMap<byte[], byte[]> {
    private static final long serialVersionUID = 1550200273310875675L;

    /**
     * Define a block map which is essentially a map of a block name (in the
     * form of bytes) to the block data (again, in the form of bytes). Given
     * that byte arrays don't lend themselves to comparison naturally, we
     * pass it a custom comparator.
     */
    public BlockMap() {
        super(CassandraFile.BYTE_ARRAY_COMPARATOR);
    }

    /**
     * Put a <key, value> tuple in the block map, where the key is a
     * {@link java.lang.String}.
     * 
     * @param key
     *            a stringified key
     * @param value
     *            a byte array value
     * @return the previously associated value
     */
    public byte[] put(String key, byte[] value) {
        return super.put(key.getBytes(), value);
    }

    /**
     * Put a <key, value> tuple in the block map, where the value is a
     * {@link java.lang.String}.
     * 
     * @param key
     *            a byte array key
     * @param value
     *            a stringified value
     * @return the previously associated value
     */
    public byte[] put(byte[] key, String value) {
        return super.put(key, value.getBytes());
    }

    /**
     * Put a <key, value> tuple in the block map, where both the key and
     * value are a {@link java.lang.String}.
     * 
     * @param key
     *            a stringified key
     * @param value
     *            a stringified value
     * @return the previously associated value
     */
    public byte[] put(String key, String value) {
        return super.put(key.getBytes(), value.getBytes());
    }

    /**
     * Get the value for the given key, which is a {@link java.lang.String}.
     * 
     * @param key
     *            a stringified key
     * @return the currently associated value
     */
    public byte[] get(String key) {
        return super.get(key.getBytes());
    }
}