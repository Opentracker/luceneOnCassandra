package org.apache.hector;

import java.util.LinkedHashMap;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

public class OpentrackerStorage {
    
    private ColumnFamilyTemplate<String, String> template;
    
    // Template like keyspace are both long life object. Ideally you would want to 
    // keep the template object in a DAO to facilitate the access to your business model.
    public OpentrackerStorage(Keyspace keyspace, String columnFamily) {
        template = new ThriftColumnFamilyTemplate<String, String>(keyspace, columnFamily, StringSerializer.get(), StringSerializer.get());
    }
    
    /**
     * add/update a column name and column value.
     * 
     * @param key
     * @param columnName
     * @param columnValue
     * @return
     */
    public boolean update(String key, String columnName, String columnValue) {
        try {
            ColumnFamilyUpdater<String, String> updater = template.createUpdater(key);
            updater.setString(columnName, columnValue);
            template.update(updater);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;  
    }
    
    /**
     * get column value based on the key and columnName.
     * 
     * @param key
     * @param columnName
     * @return
     */
    public String getColumnValue(String key, String columnName) {
        try {
            ColumnFamilyResult<String, String> res = template.queryColumns(key);
            String result = res.getString(columnName);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    /**
     * Read entire columns from a row.
     * 
     * @param keyspace
     * @param columnFamily
     * @param key
     * @return
     */
    public Map<String, String> read(Keyspace keyspace, String columnFamily, String key) {
        SliceQuery<String, String, String> query =
                HFactory.createSliceQuery(keyspace, StringSerializer.get(),
                        StringSerializer.get(), StringSerializer.get())
                        .setKey(key).setColumnFamily(columnFamily);

        ColumnSliceIterator<String, String, String> iterator =
                new ColumnSliceIterator<String, String, String>(query, null,
                        "\uFFFF", false);
        
        Map<String, String> columns = new LinkedHashMap<String, String>();
        
        while (iterator.hasNext()) {
            HColumn<String, String> column = iterator.next();
            columns.put(column.getName(), column.getValue());
        }
        return columns;
    }
    
    /**
     * Delete a row column specified by the key and columnName.
     * 
     * @param key
     * @param columnName
     * @return
     */
    public boolean deleteColumn(String key, String columnName) {
        try {
            template.deleteColumn(key, columnName);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * Delete a row specified by the key.
     * 
     * @param key
     * @return
     */
    public boolean deleteRow(String key) {
        try {
            template.deleteRow(key);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}
