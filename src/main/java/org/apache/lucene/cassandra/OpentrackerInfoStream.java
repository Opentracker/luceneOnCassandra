package org.apache.lucene.cassandra;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.InfoStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpentrackerInfoStream extends InfoStream {
    
    private static Logger logger = LoggerFactory.getLogger(OpentrackerInfoStream.class);
    
    private static final AtomicInteger MESSAGE_ID = new AtomicInteger();
    protected final int messageID;

    public OpentrackerInfoStream() {
        this.messageID = MESSAGE_ID.getAndIncrement();
    }

    @Override
    public void message(String component, String message) {
        logger.trace(component + " " + messageID + " [" + Thread.currentThread().getName() + "]: " + message);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public boolean isEnabled(String component) {
        return logger.isTraceEnabled();
    }
    
    

}
