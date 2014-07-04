package org.apache.lucene.cassandra.nio;

import sun.misc.Unsafe;

class NativeObject {
    
    protected static final Unsafe unsafe = Unsafe.getUnsafe();
    
    // Native allocation address;
    // may be smaller than the base address due to page-size rounding
    //
    protected long allocationAddress;
    
    // Native base address
    //
    private final long address;
    
    // Invoked only by AllocatedNativeObject
    //
    protected NativeObject(int size, boolean pageAligned) {
        if (!pageAligned) {
            this.allocationAddress = unsafe.allocateMemory(size);
            this.address = this.allocationAddress;
        } else {
            int ps = pageSize();
            long a = unsafe.allocateMemory(size + ps);
            this.allocationAddress = a;
            this.address = a + ps - (a & (ps - 1));
        }
    }
    
    /**
     * Returns the native base address of this native object.
     *
     * @return The native base address
     */
    long address() {
        return address;
    }
    
    // Cache for page size
    private static int pageSize = -1;
    
    /**
     * Returns the page size of the underlying hardware.
     *
     * @return  The page size, in bytes
     */
    static int pageSize() {
        if (pageSize == -1)
            pageSize = unsafe.pageSize();
        return pageSize;
    }
    
    /**
     * Writes an int at the specified offset from this native object's
     * base address.
     *
     * @param  offset
     *         The offset at which to write the int
     *
     * @param  value
     *         The int value to be written
     */
    final void putInt(int offset, int value) {
        unsafe.putInt(offset + address, value);
    }
    
    /**
     * Writes a long at the specified offset from this native object's
     * base address.
     *
     * @param  offset
     *         The offset at which to write the long
     *
     * @param  value
     *         The long value to be written
     */
    final void putLong(int offset, long value) {
        unsafe.putLong(offset + address, value);
    }
}
