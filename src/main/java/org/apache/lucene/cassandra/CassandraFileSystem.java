package org.apache.lucene.cassandra;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

public class CassandraFileSystem extends FileSystem {
    
    private final CassandraFileSystemProvider provider;
    private final byte[] defaultDirectory;
    //private final boolean needToResolveAgainstDefaultDirectory;
    private final CassandraPath rootDirectory;
    
    CassandraFileSystem(CassandraFileSystemProvider provider, String dir) {
        this.provider = provider;
        this.defaultDirectory = CassandraPath.normalizeAndCheck(dir).getBytes();
        if (this.defaultDirectory[0] != '/') {
            throw new RuntimeException("default directory must be absolute");
        }
        
        // the root directory
        this.rootDirectory = new CassandraPath(this, "/");
    }
    
    byte[] defaultDirectory() {
        return defaultDirectory;
    }
    
    /*
    boolean needToResolveAgainstDefaultDirectory() {
        return needToResolveAgainstDefaultDirectory;
    }
    */
    
    CassandraPath rootDirectory() {
        return rootDirectory;
    }
    
    static List<String> standardFileAttributeViews() {
        return Arrays.asList("basic", "posix", "unix", "owner");
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }
    
    @Override
    public String getSeparator() {
        return "/";
    }
    
    @Override
    public boolean isOpen() {
        return true;
    }
    
    @Override
    public boolean isReadOnly() {
        return false;
    }
    
    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Iterable<Path> getRootDirectories() {
        final List<Path> allowedList = Collections.unmodifiableList(Arrays.asList((Path)rootDirectory));
        return new Iterable<Path>() {
            @Override
            public Iterator<Path> iterator() {
                return allowedList.iterator();
            }
        };
    }
    
    /**
     * Returns object to iterate over entries in mounttab or equivalent
     */
    Iterable<CassandraMountEntry> getMountEntries() {
        /*
        ArrayList<CassandraMountEntry> entries = new ArrayList<>();
        try {
            long fp = setmntent("/etc/mtab".getBytes(), "r".getBytes());
            try {
                for (;;) {
                    CassandraMountEntry entry = new CassandraMountEntry();
                    int res = getmntent(fp, entry);
                    if (res < 0)
                        break;
                    entries.add(entry);
                }
            } finally {
                endmntent(fp);
            }

        } catch (Exception x) {
            // nothing we can do
        }
        return entries;
        */
        return null;
    }
    
    /**
     * Iterator returned by getFileStores method.
     */
    private class FileStoreIterator implements Iterator<FileStore> {
        private final Iterator<CassandraMountEntry> entries;
        private FileStore next;

        FileStoreIterator() {
            this.entries = getMountEntries().iterator();
        }

        private FileStore readNext() {
            /*
            assert Thread.holdsLock(this);
            for (;;) {
                if (!entries.hasNext())
                    return null;
                CassandraMountEntry entry = entries.next();

                // skip entries with the "ignore" option
                if (entry.isIgnored())
                    continue;

                // check permission to read mount point
                SecurityManager sm = System.getSecurityManager();
                if (sm != null) {
                    try {
                        sm.checkRead(new String(entry.dir()));
                    } catch (SecurityException x) {
                        continue;
                    }
                }
                try {
                    return getFileStore(entry);
                } catch (IOException ignore) {
                    // ignore as per spec
                }
            }
            */
            return null;
        }

        @Override
        public synchronized boolean hasNext() {
            if (next != null)
                return true;
            next = readNext();
            return next != null;
        }

        @Override
        public synchronized FileStore next() {
            if (next == null)
                next = readNext();
            if (next == null) {
                throw new NoSuchElementException();
            } else {
                FileStore result = next;
                next = null;
                return result;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    @Override
    public Iterable<FileStore> getFileStores() {
        return new Iterable<FileStore>() {
            
            @Override
            public Iterator<FileStore> iterator() {
                return new FileStoreIterator();
            }
        };
    }
    
    @Override
    public Path getPath(String first, String... more) {
        String path;
        if (more.length == 0) {
            path = first;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(first);
            for (String segment: more) {
                if (segment.length() > 0) {
                    if (sb.length() > 0)
                        sb.append('/');
                    sb.append(segment);
                }
            }
            path = sb.toString();
        }
        return new CassandraPath(this, path);
    }
    
    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        int pos = syntaxAndPattern.indexOf(':');
        if (pos <= 0 || pos == syntaxAndPattern.length())
            throw new IllegalArgumentException();
        String syntax = syntaxAndPattern.substring(0, pos);
        String input = syntaxAndPattern.substring(pos+1);
        
        String expr;
        if (syntax.equals(GLOB_SYNTAX)) {
            expr = Globs.toUnixRegexPattern(input);
        } else {
            if (syntax.equals(REGEX_SYNTAX)) {
                expr = input;
            } else {
                throw new UnsupportedOperationException("Syntax '" + syntax + "' not recognized");
            }
        }
        
        // return matcher
        final Pattern pattern = compilePathMatchPattern(expr);
        
        return new PathMatcher() {
            
            @Override
            public boolean matches(Path path) {
                return pattern.matcher(path.toString()).matches();
            }
        };
    }
    
    private static final String GLOB_SYNTAX = "glob";
    private static final String REGEX_SYNTAX = "regex";
    
    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        return LookupService.instance;
    }
    
    private static class LookupService {
        static final UserPrincipalLookupService instance =
                new UserPrincipalLookupService() {
                    @Override
                    public UserPrincipal lookupPrincipalByName(String name)
                        throws IOException
                    {
                        return CassandraUserPrincipals.lookupUser(name);
                    }

                    @Override
                    public GroupPrincipal lookupPrincipalByGroupName(String group)
                        throws IOException
                    {
                        return CassandraUserPrincipals.lookupGroup(group);
                    }
                };
    }
    
    // Override if the platform has different path match requrement, such as
    // case insensitive or Unicode canonical equal on MacOSX
    Pattern compilePathMatchPattern(String expr) {
        return Pattern.compile(expr);
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        // TODO Auto-generated method stub
        return null;
    }    

    @Override
    public WatchService newWatchService() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
