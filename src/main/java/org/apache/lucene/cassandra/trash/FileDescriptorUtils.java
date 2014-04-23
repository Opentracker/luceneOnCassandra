package org.apache.lucene.cassandra.trash;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.apache.lucene.store.CassandraDirectory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for serializing (and deserialize) the file descriptor to (and
 * from) JSON objects.
 */
public class FileDescriptorUtils {
    
    private static Logger logger = LoggerFactory.getLogger(FileDescriptorUtils.class);

    /**
     * Convert the given file descriptor to bytes.
     * 
     * @param fileDescriptor
     * @return
     * @throws IOException
     */
    public static byte[] toBytes(FileDescriptor fileDescriptor)
            throws IOException {
        return toString(fileDescriptor).getBytes();
    }

    /**
     * Convert the given file descriptor to a String.
     * 
     * @param fileDescriptor
     * @return
     * @throws IOException
     */
    public static String toString(FileDescriptor fileDescriptor)
            throws IOException {
        return toJSON(fileDescriptor).toString();
    }

    /**
     * Convert the given file descriptor to a {@link JSONObject}
     * 
     * @param fileDescriptor
     * @return
     * @throws IOException
     */
    public static JSONObject toJSON(FileDescriptor fileDescriptor)
            throws IOException {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", fileDescriptor.getName());
            jsonObject.put("length", fileDescriptor.getLength());
            jsonObject.put("deleted", fileDescriptor.isDeleted());
            jsonObject
                    .put("lastModified", fileDescriptor.getLastModified());
            jsonObject
                    .put("lastAccessed", fileDescriptor.getLastAccessed());
            JSONArray jsonArray = new JSONArray();
            for (FileBlock fileBlock : fileDescriptor.getBlocks()) {
                JSONObject blockObject = new JSONObject();
                blockObject.put("columnName", fileBlock.getBlockName());
                blockObject.put("blockNumber", fileBlock.getBlockNumber());
                blockObject.put("blockSize", fileBlock.getBlockSize());
                blockObject.put("dataOffset", fileBlock.getDataOffset());
                blockObject.put("dataLength", fileBlock.getDataLength());
                jsonArray.put(blockObject);
            }
            jsonObject.put("blocks", jsonArray);
            return jsonObject;
        } catch (JSONException e) {
            throw new IOException(
                    "Unable to serialize file descriptor for "
                            + fileDescriptor.getName(), e);
        }
    }

    /**
     * Convert the given bytes to a file descriptor.
     * 
     * @param descriptorBytes
     * @return
     * @throws IOException
     */
    public static FileDescriptor fromBytes(byte[] descriptorBytes)
            throws IOException {
        try {
            if (descriptorBytes == null) {
                logger.debug("descriptorBytes  is null, returning null");
                return null;
            }
            JSONTokener tokener =
                    new JSONTokener(new InputStreamReader(
                            new ByteArrayInputStream(descriptorBytes)));
            Object obj = tokener.nextValue();
            logger.debug("obj " + obj);
            FileDescriptor fileDescriptor =
                    FileDescriptorUtils.fromJSON((JSONObject) obj);
            logger.debug("isDeleted flag? {}", fileDescriptor.isDeleted());
            return (!fileDescriptor.isDeleted() ? fileDescriptor : null);
        } catch (JSONException e) {
            e.printStackTrace();
            throw new IOException("Could not get descriptor for file.", e);
        }
    }

    /**
     * Convert the given {@link JSONObject} to a file descriptor.
     * 
     * @param jsonObject
     * @return
     * @throws IOException
     */
    public static FileDescriptor fromJSON(JSONObject jsonObject)
            throws IOException {
        try {
            FileDescriptor fileDescriptor =
                    new FileDescriptor(jsonObject.getString("name"));
            fileDescriptor.setLength(jsonObject.getLong("length"));
            fileDescriptor.setDeleted(jsonObject.getBoolean("deleted"));
            fileDescriptor.setLastModified(jsonObject
                    .getLong("lastModified"));
            fileDescriptor.setLastAccessed(jsonObject
                    .getLong("lastAccessed"));
            fileDescriptor.setBlocks(new LinkedList<FileBlock>());
            JSONArray blockArray = jsonObject.getJSONArray("blocks");
            if (blockArray != null) {
                for (int index = 0; index < blockArray.length(); index++) {
                    JSONObject blockObject =
                            (JSONObject) blockArray.get(index);
                    FileBlock fileBlock = new FileBlock();
                    fileBlock.setBlockName(blockObject
                            .getString("columnName"));
                    fileBlock.setBlockNumber(blockObject
                            .getInt("blockNumber"));
                    fileBlock.setBlockSize(blockObject.getInt("blockSize"));
                    fileBlock.setDataOffset(blockObject
                            .getInt("dataOffset"));
                    fileBlock.setDataLength(blockObject
                            .getInt("dataLength"));
                    fileDescriptor.addLastBlock(fileBlock);
                }
            }
            return fileDescriptor;
        } catch (JSONException e) {
            throw new IOException(
                    "Unable to de-serialize file descriptor from "
                            + jsonObject, e);
        }
    }

    /**
     * Seek to the file block that the given file pointer positions itself
     * on.
     * 
     * @param descriptor
     *            the descriptor of the file
     * @param filePointer
     *            the pointer within the file to seek to
     * @return
     */
    public static FileBlock seekBlock(FileDescriptor descriptor,
            long filePointer) {
        long currentPointer = 0;
        for (FileBlock fileBlock : descriptor.getBlocks()) {
            if (filePointer <= currentPointer) {
                fileBlock
                        .setDataPosition((int) (filePointer - currentPointer));
                fileBlock.setBlockOffset(currentPointer);
                return fileBlock;
            }
            currentPointer += fileBlock.getDataLength();
        }
        return null;
    }
}