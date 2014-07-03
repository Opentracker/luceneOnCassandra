package org.apache.lucene.cassandra.nio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.cassandra.FileDescriptor;

import sun.nio.ch.DirectBuffer;

abstract class NativeDispatcher {
    
    abstract int read(FileDescriptor fd, long address, int len)
            throws IOException;
    
    // The number of temp buffers in our pool
    private static final int TEMP_BUF_POOL_SIZE = IOUtil.IOV_MAX;


    // Per-thread cache of temporary direct buffers
    private static ThreadLocal<BufferCache> bufferCache =
        new ThreadLocal<BufferCache>()
    {
        @Override
        protected BufferCache initialValue() {
            return new BufferCache();
        }
    };
    
    /**
     * A simple cache of direct buffers.
     */
    private static class BufferCache {
        // the array of buffers
        private ByteBuffer[] buffers;

        // the number of buffers in the cache
        private int count;

        // the index of the first valid buffer (undefined if count == 0)
        private int start;

        private int next(int i) {
            return (i + 1) % TEMP_BUF_POOL_SIZE;
        }

        BufferCache() {
            buffers = new ByteBuffer[TEMP_BUF_POOL_SIZE];
        }

        /**
         * Removes and returns a buffer from the cache of at least the given
         * size (or null if no suitable buffer is found).
         */
        ByteBuffer get(int size) {
            if (count == 0)
                return null;  // cache is empty

            ByteBuffer[] buffers = this.buffers;

            // search for suitable buffer (often the first buffer will do)
            ByteBuffer buf = buffers[start];
            if (buf.capacity() < size) {
                buf = null;
                int i = start;
                while ((i = next(i)) != start) {
                    ByteBuffer bb = buffers[i];
                    if (bb == null)
                        break;
                    if (bb.capacity() >= size) {
                        buf = bb;
                        break;
                    }
                }
                if (buf == null)
                    return null;
                // move first element to here to avoid re-packing
                buffers[i] = buffers[start];
            }

            // remove first element
            buffers[start] = null;
            start = next(start);
            count--;

            // prepare the buffer and return it
            buf.rewind();
            buf.limit(size);
            return buf;
        }

        boolean offerFirst(ByteBuffer buf) {
            if (count >= TEMP_BUF_POOL_SIZE) {
                return false;
            } else {
                start = (start + TEMP_BUF_POOL_SIZE - 1) % TEMP_BUF_POOL_SIZE;
                buffers[start] = buf;
                count++;
                return true;
            }
        }

        boolean offerLast(ByteBuffer buf) {
            if (count >= TEMP_BUF_POOL_SIZE) {
                return false;
            } else {
                int next = (start + count) % TEMP_BUF_POOL_SIZE;
                buffers[next] = buf;
                count++;
                return true;
            }
        }

        boolean isEmpty() {
            return count == 0;
        }

        ByteBuffer removeFirst() {
            assert count > 0;
            ByteBuffer buf = buffers[start];
            buffers[start] = null;
            start = next(start);
            count--;
            return buf;
        }
    }
    
    /**
     * Releases a temporary buffer by returning to the cache or freeing it. If
     * returning to the cache then insert it at the start so that it is
     * likely to be returned by a subsequent call to getTemporaryDirectBuffer.
     */
    static void offerFirstTemporaryDirectBuffer(ByteBuffer buf) {
        assert buf != null;
        BufferCache cache = bufferCache.get();
        if (!cache.offerFirst(buf)) {
            // cache is full
            free(buf);
        }
    }
    
    int pread(FileDescriptor fd, long address, int len, long position)
            throws IOException {
            throw new IOException("Operation Unsupported");
    }
    
    /**
     * Frees the memory for the given direct buffer
     */
    private static void free(ByteBuffer buf) {
        ((DirectBuffer)buf).cleaner().clean();
    }

}
