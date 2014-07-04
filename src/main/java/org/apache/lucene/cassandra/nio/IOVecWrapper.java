package org.apache.lucene.cassandra.nio;

import java.nio.ByteBuffer;

class IOVecWrapper {
    
    // Miscellaneous constants
    private static final int BASE_OFFSET = 0;
    private static final int LEN_OFFSET;
    private static final int SIZE_IOVEC;

    // The iovec array
    private final AllocatedNativeObject vecArray;
    
    // Number of elements in iovec array
    private final int size;
    
    // Buffers and position/remaining corresponding to elements in iovec array
    private final ByteBuffer[] buf;
    private final int[] position;
    private final int[] remaining;
    
    // Shadow buffers for cases when original buffer is substituted
    private final ByteBuffer[] shadow;

    // Base address of this array
    final long address;
    
    // Address size in bytes
    static int addressSize;
    
    private static class Deallocator implements Runnable {
        private final AllocatedNativeObject obj;
        Deallocator(AllocatedNativeObject obj) {
            this.obj = obj;
        }
        public void run() {
            obj.free();
        }
    }
    
    // per thread IOVecWrapper
    private static final ThreadLocal<IOVecWrapper> cached =
        new ThreadLocal<IOVecWrapper>();
    
    private IOVecWrapper(int size) {
        this.size      = size;
        this.buf       = new ByteBuffer[size];
        this.position  = new int[size];
        this.remaining = new int[size];
        this.shadow    = new ByteBuffer[size];
        this.vecArray  = new AllocatedNativeObject(size * SIZE_IOVEC, false);
        this.address   = vecArray.address();
    }
    
    static IOVecWrapper get(int size) {
        IOVecWrapper wrapper = cached.get();
        if (wrapper != null && wrapper.size < size) {
            // not big enough; eagerly release memory
            wrapper.vecArray.free();
            wrapper = null;
        }
        if (wrapper == null) {
            wrapper = new IOVecWrapper(size);
            Cleaner.create(wrapper, new Deallocator(wrapper.vecArray));
            cached.set(wrapper);
        }
        return wrapper;
    }
    
    void setBuffer(int i, ByteBuffer buf, int pos, int rem) {
        this.buf[i] = buf;
        this.position[i] = pos;
        this.remaining[i] = rem;
    }
    
    void setShadow(int i, ByteBuffer buf) {
        shadow[i] = buf;
    }
    
    ByteBuffer getBuffer(int i) {
        return buf[i];
    }
    
    void putBase(int i, long base) {
        int offset = SIZE_IOVEC * i + BASE_OFFSET;
        if (addressSize == 4)
            vecArray.putInt(offset, (int)base);
        else
            vecArray.putLong(offset, base);
    }
    
    void putLen(int i, long len) {
        int offset = SIZE_IOVEC * i + LEN_OFFSET;
        if (addressSize == 4)
            vecArray.putInt(offset, (int)len);
        else
            vecArray.putLong(offset, len);
    }
    
    int getRemaining(int i) {
        return remaining[i];
    }
    
    int getPosition(int i) {
        return position[i];
    }
    
    ByteBuffer getShadow(int i) {
        return shadow[i];
    }
    
    void clearRefs(int i) {
        buf[i] = null;
        shadow[i] = null;
    }
    
    static {
        addressSize = Util.unsafe().addressSize();
        LEN_OFFSET = addressSize;
        SIZE_IOVEC = (short) (addressSize * 2);
    }
}
