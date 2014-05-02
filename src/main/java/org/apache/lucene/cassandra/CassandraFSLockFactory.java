package org.apache.lucene.cassandra;

import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CassandraFSLockFactory extends LockFactory {
    
    private static Logger logger = LoggerFactory.getLogger(CassandraFSLockFactory.class);
    
    /**
     * Directory for the lock files.
     */
    protected CassandraFile lockDir = null;

    /**
     * Set the lock directory. This method can be only called
     * once to initialize the lock directory. It is used by {@link FSDirectory}
     * to set the lock directory to itself.
     * Subclasses can also use this method to set the directory
     * in the constructor.
     */
    protected final void setLockDir(CassandraFile lockDir) {
        logger.trace("called setLockDir");
      if (this.lockDir != null)
        throw new IllegalStateException("You can set the lock directory for this factory only once.");
      this.lockDir = lockDir;
    }
    
    /**
     * Retrieve the lock directory.
     */
    public CassandraFile getLockDir() {
       logger.trace("called getLockDir ");
      return lockDir;
    }

}
