package org.apache.lucene.cassandra.trash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.CassandraDirectory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>CassandraClient</code> encapsulates the low-level interactions of
 * the directory with the (remote) Cassandra server. In particular, it delegates
 * the request through to a Thrift client, which may or may not be framed.
 * 
 * <p>
 * Note that this class is not aware of the notions of directories and files
 * described above. Instead, it simply provides basic operations, such as those
 * that get/set columns/keys. This separation of concern of the concepts of
 * Cassandra and Lucene serves to not only keep the design simple, but also
 * readable (hopefully).
 * </p>
 */
public class CassandraClient {
    /**
     * 
     */
    public static CassandraClient CASSANDRA_CLIENT;

    // The underlying thrift client to delegate requests to.
    protected Cassandra.Client thriftClient;

    private static Logger logger = LoggerFactory
            .getLogger(CassandraClient.class);

    // The default size of a block, which in turn maps to a cassandra column.
    public static final int DEFAULT_BLOCK_SIZE = 1 * 1024 * 1024;

    // The default size of the buffer, which is managed by the index output.
    public static final int DEFAULT_BUFFER_SIZE = 1 * DEFAULT_BLOCK_SIZE;

    // The default host where the cassandra server is located.
    public static final String DEFAULT_CASSANDRA_HOST = "localhost";

    // The default port where the cassandra server is listening.
    public static final int DEFAULT_CASSANDRA_PORT = 9160;

    // The default flag indicating whether the cassandra server is framed.
    public static final boolean DEFAULT_CASSANDRA_FRAMED = true;

    // The default keyspace in which to store cassandra directories.
    public static final String DEFAULT_CASSANDRA_KEYSPACE = "lucene1";

    // The default column family
    public static final String DEFAULT_CASSANDRA_COLUMN_FAMILY = "index1";

    // The name of every column that holds a file block starts with this prefix.
    protected static final String BLOCK_COLUMN_NAME_PREFIX = "BLOCK-";

    protected static final String columnFamily =
            DEFAULT_CASSANDRA_COLUMN_FAMILY;

    protected static final String keyspace = DEFAULT_CASSANDRA_KEYSPACE;

    /**
     * Construct a Cassandra client that knows how to get/set rows/columns from
     * the given keyspace and column family, residing in the given Cassandra
     * server.
     * 
     * http://stackoverflow.com/questions/7018273/need-explanation-of-
     * transferring-binary-data-using-thrift-rpc
     * 
     * @param host
     *            the host where the Cassandra Thrift server is located
     * @param port
     *            the port where the Cassandra Thrift server is listening
     * @param framed
     *            a flag indicating whether or not to use a framed transport
     * @param cassandraDirectory
     * @throws IOException
     */
    public CassandraClient() throws IOException {
        CASSANDRA_CLIENT = this;
        new CassandraClient(DEFAULT_CASSANDRA_HOST, DEFAULT_CASSANDRA_PORT,
                DEFAULT_CASSANDRA_FRAMED);
    }

    public CassandraClient(String host, int port, boolean framed)
            throws IOException {

        // singleton
        CASSANDRA_CLIENT = this;
        logger.trace(String.format(
                "initialize cassandra client with host %s port %s framed %s",
                host, port, framed));
        TSocket socket = new TSocket(host, port);
        TTransport transport = framed ? new TFramedTransport(socket) : socket;
        try {
            transport.open();
            thriftClient = new Cassandra.Client(new TBinaryProtocol(transport));
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(IAuthenticator.USERNAME_KEY, "");
            credentials.put(IAuthenticator.PASSWORD_KEY, "");
            List<KsDef> keyspaces = thriftClient.describe_keyspaces();
            boolean createKeyspace = true;
            boolean createColumnFamily = true;

            for (KsDef ks : keyspaces) {
                if (ks.name.equals(this.keyspace)) {
                    createKeyspace = false;
                    for (CfDef cf : ks.getCf_defs()) {
                        if (cf.getName().equals(this.columnFamily)) {
                            createColumnFamily = false;
                        }
                    }
                    break;
                }
            }

            if (createKeyspace) {
                List<CfDef> cfDefs = new ArrayList<CfDef>();
                cfDefs.add(new CfDef(this.keyspace, this.columnFamily));
                KsDef ksDef =
                        new KsDef(this.keyspace,
                                "org.apache.cassandra.locator.SimpleStrategy",
                                cfDefs);
                ksDef.putToStrategy_options("replication_factor", "1");
                thriftClient.system_add_keyspace(ksDef);
            }
            thriftClient.set_keyspace(this.keyspace);
            try {
                if (createColumnFamily) {
                    CfDef cfDef = new CfDef(this.keyspace, this.columnFamily);
                    thriftClient.system_add_column_family(cfDef);
                }
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
            thriftClient.login(new AuthenticationRequest(credentials));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Unable to open connection to keyspace "
                    + this.keyspace, e);
        }
    }

    /**
     * Return the keys that define the given column names.
     * 
     * @param columnNames
     *            the names of the columns
     * @return the rows that contain those columns
     * @throws IOException
     */
    public byte[][] getKeys(List<byte[]> columnNames) throws IOException {
        try {
            List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
            for (byte[] b : columnNames) {
                converter.add(ByteBuffer.wrap(b));
            }
            List<KeySlice> keySlices =
                    thriftClient.get_range_slices(new ColumnParent()
                            .setColumn_family(this.columnFamily),
                            new SlicePredicate().setColumn_names(converter),
                            new KeyRange().setStart_key("".getBytes())
                                    .setEnd_key("".getBytes()),
                            ConsistencyLevel.ONE);
            List<byte[]> keys = new ArrayList<byte[]>();
            for (KeySlice keySlice : keySlices) {
                List<ColumnOrSuperColumn> coscs = keySlice.getColumns();
                if (coscs != null && coscs.size() == 1) {
                    ColumnOrSuperColumn cosc = coscs.get(0);
                    Column column = cosc.getColumn();
                    FileDescriptor fileDescriptor =
                            FileDescriptorUtils.fromBytes(column.getValue());
                    if (fileDescriptor == null || fileDescriptor.isDeleted()) {
                        continue;
                    }
                    keys.add(ByteBufferUtil.getArray(keySlice.key));
                }
            }
            return keys.toArray(new byte[][] {});
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Unable to list all files in "
                    + this.keyspace);
        }
    }

    /**
     * Get the given set of columns for the row specified by the given key.
     * 
     * @param key
     *            the key to the row to read from
     * @param columnNames
     *            the names of the columns to fetch
     * @return the values for those columns in that row
     * @throws IOException
     */
    public Map<byte[], byte[]> getColumns(byte[] key, Set<byte[]> columnNames)
            throws IOException {
        try {
            List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
            for (byte[] b : columnNames) {
                converter.add(ByteBuffer.wrap(b));
            }
            List<ColumnOrSuperColumn> coscs =
                    thriftClient.get_slice(ByteBuffer.wrap(key),
                            new ColumnParent(this.columnFamily),
                            new SlicePredicate().setColumn_names(converter),
                            ConsistencyLevel.ONE);
            Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
            for (ColumnOrSuperColumn cosc : coscs) {
                Column column = cosc.getColumn();
                columns.put(column.getName(), column.getValue());
            }
            return columns;
        } catch (Exception e) {
            throw new IOException(
                    "Could not read from columns for file " + key, e);
        }
    }

    /**
     * Get the given column for the row specified by the given key.
     * 
     * @param key
     *            the key to the row to read from
     * @param columnName
     *            the name of the column to fetch
     * @return the value for that column in this row
     * @throws IOException
     */
    public byte[] getColumn(byte[] fileName, byte[] columnName)
            throws IOException {
        try {
            List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
            converter.add(ByteBuffer.wrap(columnName));
            List<ColumnOrSuperColumn> coscs =
                    thriftClient.get_slice(ByteBuffer.wrap(fileName),
                            new ColumnParent()
                                    .setColumn_family(this.columnFamily),
                            new SlicePredicate().setColumn_names(converter),
                            ConsistencyLevel.ONE);
            if (!coscs.isEmpty()) {
                ColumnOrSuperColumn cosc = coscs.get(0);
                Column column = cosc.getColumn();
                return column.getValue();
            }
            return null;
        } catch (Exception e) {
            throw new IOException("Unable to read file descriptor for "
                    + fileName, e);
        }
    }

    /**
     * Set the values for the given columns in the given row.
     * 
     * @param key
     *            the key to the row being written to
     * @param columnValues
     *            the values for the columns being updated
     * @throws IOException
     */
    public void setColumns(ByteBuffer key, Map<byte[], byte[]> columnValues)
            throws IOException {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> cfMutation =
                new HashMap<String, List<Mutation>>();
        // System.out.println("key => " + ByteBufferUtil.string(key));
        logger.debug("columnValues size => {}", columnValues.size());
        mutationMap.put(key, cfMutation);

        List<Mutation> mutationList = new ArrayList<Mutation>();
        cfMutation.put(this.columnFamily, mutationList);

        if (columnValues == null || columnValues.size() == 0) {
            Mutation mutation = new Mutation();
            Deletion deletion = new Deletion();
            // try to delete column in cassandra 1.0.8
            // deletion.predicate = new SlicePredicate();
            // deletion.predicate.column_names =
            deletion.setTimestamp(System.currentTimeMillis());
            /**
             * Currently, we cannot delete rows from a column family. This issue
             * is being tracked at
             * https://issues.apache.org/jira/browse/CASSANDRA-293. When that
             * issue that resolved, we may at that time choose to revive the
             * code shown below.
             * 
             * deletion.setPredicate(new SlicePredicate().setSlice_range(new
             * SliceRange(new byte[] {}, new byte[] {}, false,
             * Integer.MAX_VALUE)));
             */
            mutation.setDeletion(deletion);
            mutationList.add(mutation);

        } else {
            for (Map.Entry<byte[], byte[]> columnValue : columnValues
                    .entrySet()) {
                Mutation mutation = new Mutation();
                byte[] column = columnValue.getKey(), value =
                        columnValue.getValue();
                logger.debug("columnName => {} columnValue => {}", new String(
                        column, "UTF-8"), new String(value, "UTF-8"));
                boolean isDelete = false;
                // yet.
                /*
                 * if (value != null) { FileDescriptor fd =
                 * FileDescriptorUtils.fromBytes(value); if (fd == null ||
                 * fd.isDeleted()) { System.out.println("entered!"); isDelete =
                 * true; } }
                 */
                if (value == null || isDelete) {
                    logger.debug("value is " + value);
                    Deletion deletion = new Deletion();
                    deletion.setTimestamp(System.currentTimeMillis());
                    // try to delete column in cassandra 1.0.8
                    deletion.predicate = new SlicePredicate();
                    deletion.predicate.column_names =
                            Arrays.asList(ByteBuffer.wrap(column));

                    if (column != null) {
                        List<ByteBuffer> converter =
                                new ArrayList<ByteBuffer>();
                        converter.add(ByteBuffer.wrap(column));
                        // deletion.setPredicate(new
                        // SlicePredicate().setColumn_names(Arrays
                        // .asList(new byte[][] {column})));
                        deletion.setPredicate(new SlicePredicate()
                                .setColumn_names(converter));
                    } else {
                        /*
                         * deletion.setPredicate(new SlicePredicate()
                         * .setSlice_range(new SliceRange(new byte[] {}, new
                         * byte[] {}, false, Integer.MAX_VALUE)));
                         */
                        deletion.setPredicate(new SlicePredicate()
                                .setSlice_range(new SliceRange(
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                        false, Integer.MAX_VALUE)));

                    }

                    mutation.setDeletion(deletion);

                } else {
                    logger.debug("value is not null");
                    ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();

                    Column nColumn = new Column();
                    nColumn.setName(ByteBuffer.wrap(column));
                    nColumn.setValue(ByteBuffer.wrap(value));
                    nColumn.setTimestamp(System.currentTimeMillis());
                    cosc.setColumn(nColumn);

                    mutation.setColumn_or_supercolumn(cosc);
                }

                mutationList.add(mutation);
            }
        }
        try {
            thriftClient.batch_mutate(mutationMap, ConsistencyLevel.ONE);
        } catch (Exception e) {
            throw new IOException("Unable to mutate columns for file " + key, e);
        }
    }
}