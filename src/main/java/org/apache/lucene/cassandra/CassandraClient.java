package org.apache.lucene.cassandra;

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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraClient {
    protected Cassandra.Client thriftClient;
    String keyspace;
    String columnFamily;
    int blockSize;
    private TTransport transport = null;
    

    public CassandraClient(String host, int port, boolean framed, String keyspace, String columnFamily, int blockSize)
            throws IOException {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.blockSize = blockSize;
        TSocket socket = new TSocket(host, port);
        socket.getSocket().setReuseAddress(true);
        socket.getSocket().setSoLinger(true, 0);
        socket.getSocket().setKeepAlive(true);
        transport = framed ? new TFramedTransport(socket) : socket;
        try {
            transport.open();
            thriftClient =
                    new Cassandra.Client(new TBinaryProtocol(transport));
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(IAuthenticator.USERNAME_KEY, "");
            credentials.put(IAuthenticator.PASSWORD_KEY, "");
            List<KsDef> keyspaces = thriftClient.describe_keyspaces();
            boolean createKeyspace = true;
            boolean createColumnFamily = true;

            for (KsDef ks : keyspaces) {
                if (ks.name.equals(keyspace)) {
                    createKeyspace = false;
                    for (CfDef cf : ks.getCf_defs()) {
                        if (cf.getName().equals(columnFamily)) {
                            createColumnFamily = false;
                        }
                    }
                    break;
                }
            }

            if (createKeyspace) {
                List<CfDef> cfDefs = new ArrayList<CfDef>();
                cfDefs.add(new CfDef(keyspace, columnFamily));
                KsDef ksDef =
                        new KsDef(
                                keyspace,
                                "org.apache.cassandra.locator.SimpleStrategy",
                                cfDefs);
                ksDef.putToStrategy_options("replication_factor", "1");
                thriftClient.system_add_keyspace(ksDef);
            }
            thriftClient.set_keyspace(keyspace);
            try {
                if (createColumnFamily) {
                    CfDef cfDef = new CfDef(keyspace, columnFamily);
                    thriftClient.system_add_column_family(cfDef);
                }
            } catch (InvalidRequestException e) {
                e.printStackTrace();
            }
            thriftClient.login(new AuthenticationRequest(credentials));
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Unable to open connection to keyspace "
                    + keyspace, e);
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
    public byte[][] getKeys(List<byte[]> columnNames, int count) throws IOException {

        try {
            List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
            for (byte[] b : columnNames) {
                converter.add(ByteBuffer.wrap(b));
            }
            List<KeySlice> keySlices =
                    thriftClient
                            .get_range_slices(
                                    new ColumnParent()
                                            .setColumn_family(columnFamily),
                                    new SlicePredicate()
                                            .setColumn_names(converter),
                                    new KeyRange().setStart_key(
                                            "".getBytes()).setEnd_key(
                                            "".getBytes()).setCount(count),
                                    ConsistencyLevel.ALL);
            List<byte[]> keys = new ArrayList<byte[]>();
            for (KeySlice keySlice : keySlices) {
                List<ColumnOrSuperColumn> coscs = keySlice.getColumns();
                if (coscs != null && coscs.size() == 1) {
                    ColumnOrSuperColumn cosc = coscs.get(0);
                    Column column = cosc.getColumn();
                    FileDescriptor fileDescriptor =
                            FileDescriptorUtils
                                    .fromBytes(column.getValue(), this.blockSize);
                    if (fileDescriptor == null
                            || fileDescriptor.isDeleted()) {
                        continue;
                    }
                    keys.add(ByteBufferUtil.getArray(keySlice.key));
                }
            }
            return keys.toArray(new byte[][] {});
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Unable to list all files in " + keyspace);
        }
    }

    /**
     * Get all the columns which belong to the key.
     *
     * @param key
     *            the row key.
     *
     * @return all the columns belong to the key.
     *
     * @throws IOException
     */
    public Map<byte[], byte[]> getColumns(byte[] key) throws IOException {
        try {
            SliceRange sliceRange = new SliceRange();
            sliceRange.setStart(new byte[0]);
            sliceRange.setFinish(new byte[0]);
            List<ColumnOrSuperColumn> coscs =
                    thriftClient.get_slice(ByteBuffer.wrap(key),
                            new ColumnParent(columnFamily),
                            new SlicePredicate().setSlice_range(sliceRange),
                            ConsistencyLevel.ALL);
            Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
            for (ColumnOrSuperColumn cosc : coscs) {
                Column column = cosc.getColumn();
                columns.put(column.getName(), column.getValue());
            }
            return columns;
        } catch (Exception e) {
            throw new IOException("Could not read from columns for file "
                    + Util.hexToAscii(Util.bytesToHex(key)), e);
        }
    }

    /**
     * Get the given set of columns for the row specified by the given key.
     * 
     * populate columnname and column value based on the key and column name
     * specified.
     * 
     * @param key
     *            the key to the row to read from
     * @param columnNames
     *            the names of the columns to fetch
     * @return the values for those columns in that row
     * @throws IOException
     */
    public Map<byte[], byte[]> getColumns(byte[] key,
            Set<byte[]> columnNames) throws IOException {
        try {
            List<ByteBuffer> converter = new ArrayList<ByteBuffer>();
            for (byte[] b : columnNames) {
                converter.add(ByteBuffer.wrap(b));
            }
            List<ColumnOrSuperColumn> coscs =
                    thriftClient
                            .get_slice(ByteBuffer.wrap(key),
                                    new ColumnParent(columnFamily),
                                    new SlicePredicate()
                                            .setColumn_names(converter),
                                    ConsistencyLevel.ALL);
            Map<byte[], byte[]> columns = new HashMap<byte[], byte[]>();
            for (ColumnOrSuperColumn cosc : coscs) {
                Column column = cosc.getColumn();
                columns.put(column.getName(), column.getValue());
            }
            return columns;
        } catch (Exception e) {
            throw new IOException("Could not read from columns for file "
                    + Util.hexToAscii(Util.bytesToHex(key)), e);
        }
    }

    /**
     * Get the column value for the row specified by the given key and columnName
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
                    thriftClient
                            .get_slice(
                                    ByteBuffer.wrap(fileName),
                                    new ColumnParent()
                                            .setColumn_family(columnFamily),
                                    new SlicePredicate()
                                            .setColumn_names(converter),
                                    ConsistencyLevel.ALL);
            if (!coscs.isEmpty()) {
                ColumnOrSuperColumn cosc = coscs.get(0);
                Column column = cosc.getColumn();
                return column.getValue();
            }
            return null;
        } catch (Exception e) {
            throw new IOException("Unable to read file descriptor for "
                    + Util.hexToAscii(Util.bytesToHex(fileName)), e);
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
    public void setColumns(ByteBuffer key,
            Map<byte[], byte[]> columnValues) throws IOException {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap =
                new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> cfMutation =
                new HashMap<String, List<Mutation>>();
        // System.out.println("key => " + ByteBufferUtil.string(key));
        mutationMap.put(key, cfMutation);

        List<Mutation> mutationList = new ArrayList<Mutation>();
        cfMutation.put(columnFamily, mutationList);

        if (columnValues == null || columnValues.size() == 0) {
            Mutation mutation = new Mutation();
            Deletion deletion = new Deletion();
            // try to delete column in cassandra 1.0.8
            // deletion.predicate = new SlicePredicate();
            // deletion.predicate.column_names =
            deletion.setTimestamp(System.currentTimeMillis());
            /**
             * Currently, we cannot delete rows from a column family. This
             * issue is being tracked at
             * https://issues.apache.org/jira/browse/CASSANDRA-293. When
             * that issue that resolved, we may at that time choose to
             * revive the code shown below.
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
                boolean isDelete = false;
                if (value == null || isDelete) {
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
            thriftClient.batch_mutate(mutationMap, ConsistencyLevel.ALL);
        } catch (Exception e) {
            throw new IOException("Unable to mutate columns for file "
                    + new String(key.array(), "UTF-8"), e);
        }
    }
    
    public boolean truncate(String cfname) throws IOException {
        try {
            thriftClient.truncate(cfname);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    public void close()  {
        transport.close();
    }
    
    // dont know why always get broken pipe although already set socket to keep
    // alive. we want flush because we want the data to be persistent as much as
    // possible.
    public void flush() throws TTransportException {
        //transport.flush();
    }
}