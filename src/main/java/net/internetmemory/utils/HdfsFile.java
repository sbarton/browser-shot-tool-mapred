/*
 * Representation and services of a file node in Hdfs
 * 
 */
package net.internetmemory.utils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileAlreadyExistsException;

/**
 * This class represents a "file", more generalluy a node in a HDFS hierarchy
 * 
 */
public class HdfsFile {

    /**
     * The FileSystem handler
     */
    protected FileSystem fs;
    /**
     * The full path to the directory
     * 
     */
    protected Path path;
    /**
     * The status
     */
    protected FileStatus status;
    
    /**
     * Output stream to write in the file
     */
    private FSDataOutputStream FDataOutputStream;

 
    /**
     * Constructor
     * 
     * The method sends an exception if the file does not exist.     
     * 
     * @param theFs The file system the directory belongs to
     * @param thePath The absolute path to the directory
     * 
     * @throws ZooKeeperConnectionException 
     */
    public HdfsFile(FileSystem theFs, Path thePath) throws IOException {
        fs = theFs;
        path = thePath;
        try {
            // Check that the path exists
            if (!fs.exists(path)) {
                throw new IOException("Cannot instantiate HDFS file " + path
                        + " : path does not exists.");
            }
            status = fs.getFileStatus(path);
        } catch (Exception ex) {
            throw new IOException("Unable to instantiate file " + path.toString()
                    + ": " + ex.getMessage());
        }
    }

    /**
     * Constructor with create and check options
     * 
     * The method sends an exception if the file does not exist.     
     * 
     * @param theFs The file system the directory belongs to
     * @param thePath The absolute path to the directory
     * @param create True if the file should be created if does not exist
     * @param strict If file should be created and it exists, throw exception
     * 
     * @throws ZooKeeperConnectionException 
     */
    public HdfsFile(FileSystem theFs, Path thePath, boolean create, boolean strict) throws IOException {
        fs = theFs;
        path = thePath;
        try {
            // Check that the path exists
            if (!fs.exists(path)) {
                if (create) {
                    FDataOutputStream = fs.create(path);
                    status = fs.getFileStatus(path);
                } else {
                    throw new IOException("Cannot instantiate HDFS file " + path
                            + " : path does not exists.");
                }
            } else {
                if (strict) {
                    throw new FileAlreadyExistsException("File already exists: " + path);
                } else {
                    fs.delete(path, false);
                    FDataOutputStream = fs.create(path);
                    status = fs.getFileStatus(path);
                }
            }
        } catch (Exception ex) {
            throw new IOException("Unable to instantiate file " + path.toString()
                    + ": " + ex.getMessage());
        }
    }

    /**
     * Get the (absolute) path 
     */
    public Path getPath() {
        return path;
    }

    /**
     * Get the basename path (ie., the local name)
     */
    String getBasePath() {
        return path.getName();
    }

    /**
     * Get the parent directory, null if root 
     */
    public Path getParentDir() {
        return path.getParent();
    }

    /**
     * Check whether the file is a directory
     */
    public boolean isDir() {
        return status.isDir();
    }

    /**
     * Delete a file
     * 
     * 
     * @return true or false, depending on success / failure
     */
    public boolean delete() throws IOException {
        if (!isDir()) {
            // The directory is now empty so delete it
            return fs.delete(path, true);
        } else {
            // Cannot delete a dir seen as a file
            return false;
        }
    }

    /**
     * Append some content to the file
     */
    public void append(BufferedInputStream buf) throws IOException {
        // Get the output stream on the file content
        FSDataOutputStream out = fs.append(path);
        int i;
        do {
            i = buf.read();
            if (i != -1) {
                out.write(i);
            }
        } while (i != -1);
        out.flush();
        out.close();
    }

    /**
     * Append some content to the file
     * 
     */
    public long append(InputStream is) throws IOException {
        // Get the output stream on the file content
        FSDataOutputStream out = fs.append(path);
        return flushToFile(out, is);
    }

    
    public FSDataOutputStream getOutputStreamForAppend() throws IOException {
        FSDataOutputStream out = fs.append(path);
        return out;
    }
    
    /**
     * Get output stream with the current path
     * 
     * @return An output stream over the file content
     * 
     * @throws IOException 
     */
    public FSDataOutputStream getOutputStream() throws IOException {
        if (FDataOutputStream != null) return FDataOutputStream;
        FSDataOutputStream out = fs.create(path);
        return out;
    }
    
    /**
     * Append some content to the file
     * 
     */
    public long create(InputStream is) throws IOException {
        // Get the output stream on the file content
        //FDataOutputStream = fs.create(path);        
        return flushToFile(FDataOutputStream, is);
    }

    /**
     * Get input stream to read file
     * @return input stream for the file
     * @throws IOException 
     */
    public FSDataInputStream getInputStream() throws IOException {
        return fs.open(path);
    }

    /**
     * Get the content of the file as a String
     */
    public byte[] getContent() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        InputStream stream = fs.open(path);
        byte[] buffer = new byte[1024];
        int l;
        do {
            l = (stream.read(buffer));
            if (l > 0) {
                out.write(buffer, 0, l);
            }
        } while (l > 0);
        return out.toByteArray();
    }

    /**
     * Flush input stream to file
     * @param fileOutputStream
     * @param data
     * @return
     * @throws IOException 
     */
    private long flushToFile(FSDataOutputStream fileOutputStream, InputStream data) throws IOException {
        long totalRead = 0;
        int read = -1;
        byte[] buffer = new byte[256*1024];
        //int howManyReads = 0;
        while ((read = data.read(buffer, 0, buffer.length)) != -1) {
            // put the buffer into Fileoutputstream
//System.out.println(howManyReads++ + " : " +read + " : "+totalRead);   
            fileOutputStream.write(buffer, 0, read);
            totalRead += read;
        }
        fileOutputStream.flush();
        fileOutputStream.close();
        return totalRead;
    }

    /**
     * Get the size of the file, in bytes
     */
    public long getSize() {
        return status.getLen();
    }

  /**
     * Get the modification date of the file
      */
    public long getModificationTime() {
        return status.getModificationTime();
    }

    /**
     * Move/rename file
     * @param path to move the file to
     * @return true if moved false if not
     * @throws IOException 
     */
    public boolean move(Path path) throws IOException {
        boolean rename = fs.rename(this.path, path);
        this.path = path;
        this.status = fs.getFileStatus(path);
        return rename;
    }
    
    /**
     * Sets permision to current path
     * @param permission
     * @throws IOException 
     */
    public void setPermission(FsPermission permission) throws IOException {
        fs.setPermission(path, permission);
    }
    
    /**
     * Sets replication factor to current path
     */
    public boolean setReplication(short replication) throws IOException {
        return fs.setReplication(path, replication);
    }
    
    /**
     * Checks whether file denoted by this path exists.
     * @return true if file exists, false otherwise
     */
    public boolean exists() {
        try {
            return fs.exists(path);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFile.class.getName()).log(Level.SEVERE, ex.getLocalizedMessage());
            return false;
        }
    }
    
    /**
     * Close the underlying stream
     */
    public void close() {
        if (FDataOutputStream != null) {
            try {
                FDataOutputStream.close();
            } catch (IOException ex) {
                Logger.getLogger(HdfsFile.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
