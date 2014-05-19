package org.apache.lucene.cassandra.nio;

import java.io.File;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Iterator;

public class CassandraPath implements Path  {
    
    private final CassandraFileSystem fs;
    
    private static ThreadLocal<SoftReference<CharsetEncoder>> encoder =
            new ThreadLocal<SoftReference<CharsetEncoder>>();
    
    // internal representation
    private final byte[] path;
    
    public CassandraPath(CassandraFileSystem fs, byte[] path) {
        this.fs = fs;
        this.path = path;
    }

    public CassandraPath(CassandraFileSystem fs, String input) {
        // removes redundant slashes and checks for invalid characters
        this(fs, encode(fs, normalizeAndCheck(input)));
    }

    public static String normalizeAndCheck(String input) {
        int n = input.length();
        char prevChar = 0;
        for (int i = 0; i < n; i++) {
            char c = input.charAt(i);
            if ((c == '/') && (prevChar == '/'))
                return normalize(input, n, i -1);
            checkNotNul(input, c);
            prevChar = c;
        }
        if (prevChar == '/')
            return normalize(input, n, n - 1);
        return input;
    }
    
    private static void checkNotNul(String input, char c) {
        if (c == '\u0000')
            throw new InvalidPathException(input, "Nul character not allowed");
    }
    
    private static String normalize(String input, int len, int off) {
        if (len == 0)
            return input;
        int n = len;
        while ((n > 0) && (input.charAt(n - 1) == '/')) n--;
        if (n == 0)
            return "/";
        StringBuilder sb = new StringBuilder(input.length());
        if (off > 0)
            sb.append(input.substring(0, off));
        char prevChar = 0;
        for (int i=off; i < n; i++) {
            char c = input.charAt(i);
            if ((c == '/') && (prevChar == '/'))
                continue;
            checkNotNul(input, c);
            sb.append(c);
            prevChar = c;
        }
        return sb.toString();
    }
    
    // encodes the given path-string into a sequence of bytes
    private static byte[] encode(CassandraFileSystem fs, String input) {
        SoftReference<CharsetEncoder> ref = encoder.get();
        CharsetEncoder ce = (ref != null) ? ref.get() : null;
        if (ce == null) {
            ce = Charset.defaultCharset().newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
            encoder.set(new SoftReference<CharsetEncoder>(ce));
        }

        char[] ca = fs.normalizeNativePath(input.toCharArray());

        // size output buffer for worse-case size
        byte[] ba = new byte[(int)(ca.length * (double)ce.maxBytesPerChar())];

        // encode
        ByteBuffer bb = ByteBuffer.wrap(ba);
        CharBuffer cb = CharBuffer.wrap(ca);
        ce.reset();
        CoderResult cr = ce.encode(cb, bb, true);
        boolean error;
        if (!cr.isUnderflow()) {
            error = true;
        } else {
            cr = ce.flush(bb);
            error = !cr.isUnderflow();
        }
        if (error) {
            throw new InvalidPathException(input,
                "Malformed input or input contains unmappable chacraters");
        }

        // trim result to actual length if required
        int len = bb.position();
        if (len != ba.length)
            ba = Arrays.copyOf(ba, len);

        return ba;
    }

    @Override
    public CassandraFileSystem getFileSystem() {
        return fs;
    }

    @Override
    public boolean isAbsolute() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Path getRoot() {
        // TODO Auto-generated method stub
        return null;
    }
    
    // package-private
    byte[] asByteArray() {
        return path;
    }
    
    // use this path for permission checks
    String getPathForPermissionCheck() {
        if (getFileSystem().needToResolveAgainstDefaultDirectory()) {
            return new String(getByteArrayForSysCalls());
        } else {
            return toString();
        }
    }
    
    // use this path when making system/library calls
    byte[] getByteArrayForSysCalls() {
        // resolve against default directory if required (chdir allowed or
        // file system default directory is not working directory)
        if (getFileSystem().needToResolveAgainstDefaultDirectory()) {
            return resolve(getFileSystem().defaultDirectory(), path);
        } else {
            if (!isEmpty()) {
                return path;
            } else {
                // empty path case will access current directory
                byte[] here = { '.' };
                return here;
            }
        }
    }
    
    // returns {@code true} if this path is an empty path
    private boolean isEmpty() {
        return path.length == 0;
    }
    
    // Resolve child against given base
    private static byte[] resolve(byte[] base, byte[] child) {
        int baseLength = base.length;
        int childLength = child.length;
        if (childLength == 0)
            return base;
        if (baseLength == 0 || child[0] == '/')
            return child;
        byte[] result;
        if (baseLength == 1 && base[0] == '/') {
            result = new byte[childLength + 1];
            result[0] = '/';
            System.arraycopy(child, 0, result, 1, childLength);
        } else {
            result = new byte[baseLength + 1 + childLength];
            System.arraycopy(base, 0, result, 0, baseLength);
            result[base.length] = '/';
            System.arraycopy(child, 0, result, baseLength+1, childLength);
        }
        return result;
    }

    @Override
    public Path getFileName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path getParent() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getNameCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Path getName(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean startsWith(Path other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean startsWith(String other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean endsWith(Path other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean endsWith(String other) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Path normalize() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolve(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolve(String other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolveSibling(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path resolveSibling(String other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path relativize(Path other) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public URI toUri() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path toAbsolutePath() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public File toFile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events,
            Modifier... modifiers) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>... events)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<Path> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int compareTo(Path other) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String getPathForExceptionMessage() {
        return toString();
    }

}
