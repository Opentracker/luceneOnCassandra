package org.apache.lucene.cassandra.nio;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public class Cleaner extends PhantomReference {
    
    // Dummy reference queue, needed because the PhantomReference constructor
    // insists that we pass a queue.  Nothing will ever be placed on this queue
    // since the reference handler invokes cleaners explicitly.
    //
    private static final ReferenceQueue dummyQueue = new ReferenceQueue();
    
    // Doubly-linked list of live cleaners, which prevents the cleaners
    // themselves from being GC'd before their referents
    //
    static private Cleaner first = null;
    
    private Cleaner
    next = null,
    prev = null;
    
    private final Runnable thunk;
    
    private Cleaner(Object referent, Runnable thunk) {
        super(referent, dummyQueue);
        this.thunk = thunk;
    }
    
    private static synchronized Cleaner add(Cleaner cl) {
        if (first != null) {
            cl.next = first;
            first.prev = cl;
        }
        first = cl;
        return cl;
    }
    
    /**
     * Creates a new cleaner.
     *
     * @param  thunk
     *         The cleanup code to be run when the cleaner is invoked.  The
     *         cleanup code is run directly from the reference-handler thread,
     *         so it should be as simple and straightforward as possible.
     *
     * @return  The new cleaner
     */
    public static Cleaner create(Object ob, Runnable thunk) {
        if (thunk == null)
            return null;
        return add(new Cleaner(ob, thunk));
    }

}
