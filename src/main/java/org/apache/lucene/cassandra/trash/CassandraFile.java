package org.apache.lucene.cassandra.trash;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

import org.apache.lucene.store.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract representation of file and directory pathnames.
 * 
 * <p>
 * User interfaces and operating systems use system-dependent <em>pathname
 * strings</em> to name files and directories. This class presents an abstract,
 * system-independent view of hierarchical pathnames. An
 * <em>abstract pathname</em> has two components:
 * 
 * <ol>
 * <li>An optional system-dependent <em>prefix</em> string, such as a disk-drive
 * specifier, <code>"/"</code>&nbsp;for the UNIX root directory, or
 * <code>"\\\\"</code>&nbsp;for a Microsoft Windows UNC pathname, and
 * <li>A sequence of zero or more string <em>names</em>.
 * </ol>
 * 
 * The first name in an abstract pathname may be a directory name or, in the
 * case of Microsoft Windows UNC pathnames, a hostname. Each subsequent name in
 * an abstract pathname denotes a directory; the last name may denote either a
 * directory or a file. The <em>empty</em> abstract pathname has no prefix and
 * an empty name sequence.
 * 
 * <p>
 * The conversion of a pathname string to or from an abstract pathname is
 * inherently system-dependent. When an abstract pathname is converted into a
 * pathname string, each name is separated from the next by a single copy of the
 * default <em>separator character</em>. The default name-separator character is
 * defined by the system property <code>file.separator</code>, and is made
 * available in the public static fields <code>{@link
 * #separator}</code> and <code>{@link #separatorChar}</code> of this class.
 * When a pathname string is converted into an abstract pathname, the names
 * within it may be separated by the default name-separator character or by any
 * other name-separator character that is supported by the underlying system.
 * 
 * <p>
 * A pathname, whether abstract or in string form, may be either
 * <em>absolute</em> or <em>relative</em>. An absolute pathname is complete in
 * that no other information is required in order to locate the file that it
 * denotes. A relative pathname, in contrast, must be interpreted in terms of
 * information taken from some other pathname. By default the classes in the
 * <code>java.io</code> package always resolve relative pathnames against the
 * current user directory. This directory is named by the system property
 * <code>user.dir</code>, and is typically the directory in which the Java
 * virtual machine was invoked.
 * 
 * <p>
 * The <em>parent</em> of an abstract pathname may be obtained by invoking the
 * {@link #getParent} method of this class and consists of the pathname's prefix
 * and each name in the pathname's name sequence except for the last. Each
 * directory's absolute pathname is an ancestor of any <tt>File</tt> object with
 * an absolute abstract pathname which begins with the directory's absolute
 * pathname. For example, the directory denoted by the abstract pathname
 * <tt>"/usr"</tt> is an ancestor of the directory denoted by the pathname
 * <tt>"/usr/local/bin"</tt>.
 * 
 * <p>
 * The prefix concept is used to handle root directories on UNIX platforms, and
 * drive specifiers, root directories and UNC pathnames on Microsoft Windows
 * platforms, as follows:
 * 
 * <ul>
 * 
 * <li>For UNIX platforms, the prefix of an absolute pathname is always
 * <code>"/"</code>. Relative pathnames have no prefix. The abstract pathname
 * denoting the root directory has the prefix <code>"/"</code> and an empty name
 * sequence.
 * 
 * <li>For Microsoft Windows platforms, the prefix of a pathname that contains a
 * drive specifier consists of the drive letter followed by <code>":"</code> and
 * possibly followed by <code>"\\"</code> if the pathname is absolute. The
 * prefix of a UNC pathname is <code>"\\\\"</code>; the hostname and the share
 * name are the first two names in the name sequence. A relative pathname that
 * does not specify a drive has no prefix.
 * 
 * </ul>
 * 
 * <p>
 * Instances of this class may or may not denote an actual file-system object
 * such as a file or a directory. If it does denote such an object then that
 * object resides in a <i>partition</i>. A partition is an operating
 * system-specific portion of storage for a file system. A single storage device
 * (e.g. a physical disk-drive, flash memory, CD-ROM) may contain multiple
 * partitions. The object, if any, will reside on the partition <a
 * name="partName">named</a> by some ancestor of the absolute form of this
 * pathname.
 * 
 * <p>
 * A file system may implement restrictions to certain operations on the actual
 * file-system object, such as reading, writing, and executing. These
 * restrictions are collectively known as <i>access permissions</i>. The file
 * system may have multiple sets of access permissions on a single object. For
 * example, one set may apply to the object's <i>owner</i>, and another may
 * apply to all other users. The access permissions on an object may cause some
 * methods in this class to fail.
 * 
 * <p>
 * Instances of the <code>File</code> class are immutable; that is, once
 * created, the abstract pathname represented by a <code>File</code> object will
 * never change.
 * 
 * @author unascribed
 * @since JDK1.0
 */

public class CassandraFile implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(CassandraFile.class);
    // A comparator that checks if two byte arrays are the same or not.
    protected static final Comparator<byte[]> BYTE_ARRAY_COMPARATOR =
            new Comparator<byte[]>() {
                public int compare(byte[] o1, byte[] o2) {
                    if (o1 == null || o2 == null) {
                        return (o1 != null ? -1 : o2 != null ? 1 : 0);
                    }
                    if (o1.length != o2.length) {
                        return o1.length - o2.length;
                    }
                    for (int i = 0; i < o1.length; i++) {
                        if (o1[i] != o2[i]) {
                            return o1[i] - o2[i];
                        }
                    }
                    return 0;
                }
            };

    /**
     * Creates a new <code>File</code> instance from a parent abstract pathname
     * and a child pathname string.
     * 
     * <p>
     * If <code>parent</code> is <code>null</code> then the new
     * <code>File</code> instance is created as if by invoking the
     * single-argument <code>File</code> constructor on the given
     * <code>child</code> pathname string.
     * 
     * <p>
     * Otherwise the <code>parent</code> abstract pathname is taken to denote a
     * directory, and the <code>child</code> pathname string is taken to denote
     * either a directory or a file. If the <code>child</code> pathname string
     * is absolute then it is converted into a relative pathname in a
     * system-dependent way. If <code>parent</code> is the empty abstract
     * pathname then the new <code>File</code> instance is created by converting
     * <code>child</code> into an abstract pathname and resolving the result
     * against a system-dependent default directory. Otherwise each pathname
     * string is converted into an abstract pathname and the child abstract
     * pathname is resolved against the parent.
     * 
     * @param parent
     *            The parent abstract pathname
     * @param child
     *            The child pathname string
     * @throws NullPointerException
     *             If <code>child</code> is <code>null</code>
     */
    public CassandraFile(CassandraFile file, String file2) {
    }

    /**
     * Creates a new <code>File</code> instance by converting the given pathname
     * string into an abstract pathname. If the given string is the empty
     * string, then the result is the empty abstract pathname.
     * 
     * @param pathname
     *            A pathname string
     * @throws NullPointerException
     *             If the <code>pathname</code> argument is <code>null</code>
     */
    public CassandraFile(String canonicalPath) {
    }

    /**
     * Tests whether the file or directory denoted by this abstract pathname
     * exists.
     * 
     * @return <code>true</code> if and only if the file or directory denoted by
     *         this abstract pathname exists; <code>false</code> otherwise
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *             method denies read access to the file or directory
     */
    public boolean exists() {
        return false;
    }

    /**
     * Atomically creates a new, empty file named by this abstract pathname if
     * and only if a file with this name does not yet exist. The check for the
     * existence of the file and the creation of the file if it does not exist
     * are a single operation that is atomic with respect to all other
     * filesystem activities that might affect the file.
     * <P>
     * Note: this method should <i>not</i> be used for file-locking, as the
     * resulting protocol cannot be made to work reliably. The
     * {@link java.nio.channels.FileLock FileLock} facility should be used
     * instead.
     * 
     * @return <code>true</code> if the named file does not exist and was
     *         successfully created; <code>false</code> if the named file
     *         already exists
     * 
     * @throws IOException
     *             If an I/O error occurred
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>
     *             {@link java.lang.SecurityManager#checkWrite(java.lang.String)}
     *             </code> method denies write access to the file
     * 
     * @since 1.2
     */
    public boolean createNewFile() {
        return false;
    }

    /**
     * Deletes the file or directory denoted by this abstract pathname. If this
     * pathname denotes a directory, then the directory must be empty in order
     * to be deleted.
     * 
     * @return <code>true</code> if and only if the file or directory is
     *         successfully deleted; <code>false</code> otherwise
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkDelete}</code> method denies
     *             delete access to the file
     */
    public boolean delete() {
        return false;
    }

    /**
     * Tests whether the file denoted by this abstract pathname is a directory.
     * 
     * @return <code>true</code> if and only if the file denoted by this
     *         abstract pathname exists <em>and</em> is a directory;
     *         <code>false</code> otherwise
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *             method denies read access to the file
     */
    public boolean isDirectory() {
        return true;
    }

    /**
     * Returns the absolute pathname string of this abstract pathname.
     * 
     * <p>
     * If this abstract pathname is already absolute, then the pathname string
     * is simply returned as if by the <code>{@link #getPath}</code> method. If
     * this abstract pathname is the empty abstract pathname then the pathname
     * string of the current user directory, which is named by the system
     * property <code>user.dir</code>, is returned. Otherwise this pathname is
     * resolved in a system-dependent way. On UNIX systems, a relative pathname
     * is made absolute by resolving it against the current user directory. On
     * Microsoft Windows systems, a relative pathname is made absolute by
     * resolving it against the current directory of the drive named by the
     * pathname, if any; if not, it is resolved against the current user
     * directory.
     * 
     * @return The absolute pathname string denoting the same file or directory
     *         as this abstract pathname
     * 
     * @throws SecurityException
     *             If a required system property value cannot be accessed.
     * 
     * @see java.io.File#isAbsolute()
     */
    public String getAbsolutePath() {
        return "/";
    }

    /**
     * Returns the canonical pathname string of this abstract pathname.
     * 
     * <p>
     * A canonical pathname is both absolute and unique. The precise definition
     * of canonical form is system-dependent. This method first converts this
     * pathname to absolute form if necessary, as if by invoking the
     * {@link #getAbsolutePath} method, and then maps it to its unique form in a
     * system-dependent way. This typically involves removing redundant names
     * such as <tt>"."</tt> and <tt>".."</tt> from the pathname, resolving
     * symbolic links (on UNIX platforms), and converting drive letters to a
     * standard case (on Microsoft Windows platforms).
     * 
     * <p>
     * Every pathname that denotes an existing file or directory has a unique
     * canonical form. Every pathname that denotes a nonexistent file or
     * directory also has a unique canonical form. The canonical form of the
     * pathname of a nonexistent file or directory may be different from the
     * canonical form of the same pathname after the file or directory is
     * created. Similarly, the canonical form of the pathname of an existing
     * file or directory may be different from the canonical form of the same
     * pathname after the file or directory is deleted.
     * 
     * @return The canonical pathname string denoting the same file or directory
     *         as this abstract pathname
     * 
     * @throws IOException
     *             If an I/O error occurs, which is possible because the
     *             construction of the canonical pathname may require filesystem
     *             queries
     * 
     * @throws SecurityException
     *             If a required system property value cannot be accessed, or if
     *             a security manager exists and its <code>
     *             {@link java.lang.SecurityManager#checkRead}</code> method
     *             denies read access to the file
     * 
     * @since JDK1.1
     */
    public String getCanonicalPath() {
        return "/";
    }

    /**
     * Converts this abstract pathname into a pathname string. The resulting
     * string uses the {@link #separator default name-separator character} to
     * separate the names in the name sequence.
     * 
     * @return The string form of this abstract pathname
     */
    public String getPath() {
        return "/";
    }

    /**
     * Returns an array of strings naming the files and directories in the
     * directory denoted by this abstract pathname that satisfy the specified
     * filter. The behavior of this method is the same as that of the
     * {@link #list()} method, except that the strings in the returned array
     * must satisfy the filter. If the given {@code filter} is {@code null} then
     * all names are accepted. Otherwise, a name satisfies the filter if and
     * only if the value {@code true} results when the
     * {@link FilenameFilter#accept FilenameFilter.accept(File,&nbsp;String)}
     * method of the filter is invoked on this abstract pathname and the name of
     * a file or directory in the directory that it denotes.
     * 
     * @param filter
     *            A filename filter
     * 
     * @return An array of strings naming the files and directories in the
     *         directory denoted by this abstract pathname that were accepted by
     *         the given {@code filter}. The array will be empty if the
     *         directory is empty or if no names were accepted by the filter.
     *         Returns {@code null} if this abstract pathname does not denote a
     *         directory, or if an I/O error occurs.
     * 
     * @throws SecurityException
     *             If a security manager exists and its
     *             {@link SecurityManager#checkRead(String)} method denies read
     *             access to the directory
     */
    public String[] list(FilenameFilter filenameFilter) {
        return null;
    }

    /**
     * Returns the time that the file denoted by this abstract pathname was last
     * modified.
     * 
     * @return A <code>long</code> value representing the time the file was last
     *         modified, measured in milliseconds since the epoch (00:00:00 GMT,
     *         January 1, 1970), or <code>0L</code> if the file does not exist
     *         or if an I/O error occurs
     * @throws IOException
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *             method denies read access to the file
     */
    public long lastModified() throws IOException {
        ensureOpen();
        logger.trace("fileModified " + fileName);
        org.apache.lucene.cassandra.trash.FileDescriptor descriptor =
                ColumnOrientedDirectory.SINGLETON.getFileDescriptor(fileName);
        if (descriptor == null) {
            throw new IOException("Could not find descriptor for file "
                    + fileName);
        }
        return descriptor.getLastModified();
    }

    /**
     * Returns the length of the file denoted by this abstract pathname. The
     * return value is unspecified if this pathname denotes a directory.
     * 
     * @return The length, in bytes, of the file denoted by this abstract
     *         pathname, or <code>0L</code> if the file does not exist. Some
     *         operating systems may return <code>0L</code> for pathnames
     *         denoting system-dependent entities such as devices or pipes.
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *             method denies read access to the file
     */
    String fileName;

    public long length() throws IOException {
        ensureOpen();
        logger.trace("fileLength " + fileName);
        org.apache.lucene.cassandra.trash.FileDescriptor descriptor =
                ColumnOrientedDirectory.SINGLETON.getFileDescriptor(fileName);
        if (descriptor == null) {
            throw new IOException("Could not find descriptor for file "
                    + fileName);
        }
        return descriptor.getLength();
    }

    // A flag indicating whether or not this stream is open.
    private volatile boolean isOpen;

    protected final void ensureOpen() throws AlreadyClosedException {
        logger.trace("ensureOpen ");
        if (!isOpen)
            throw new AlreadyClosedException("this Directory is closed");
    }

    /**
     * Creates the directory named by this abstract pathname, including any
     * necessary but nonexistent parent directories. Note that if this operation
     * fails it may have succeeded in creating some of the necessary parent
     * directories.
     * 
     * @return <code>true</code> if and only if the directory was created, along
     *         with all necessary parent directories; <code>false</code>
     *         otherwise
     * 
     * @throws SecurityException
     *             If a security manager exists and its <code>{@link
     *          java.lang.SecurityManager#checkRead(java.lang.String)}</code>
     *             method does not permit verification of the existence of the
     *             named directory and all necessary parent directories; or if
     *             the <code>{@link
     *          java.lang.SecurityManager#checkWrite(java.lang.String)}</code>
     *             method does not permit the named directory and all necessary
     *             parent directories to be created
     */
    public boolean mkdirs() {
        return true;
    }

}
