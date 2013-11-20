/*
 * Representation and services on a directory in HDFS
 * 
 */
package net.internetmemory.utils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * This class represents a directory
 * 
 */
public class HdfsDir extends HdfsFile {

    /**
     * Constructor
     * 
     * @param theFs The file system the directory belongs to
     * @param thePath The absolute path to the directory
     * 
     * @throws ZooKeeperConnectionException 
     */
    public HdfsDir(FileSystem theFs, Path thePath) throws IOException {
        super(theFs, thePath);
    }

       /**
     * Get the list of sub directories
     * 
     * @return List of subdir paths 
     * @throws IOException 
     */
    public List<HdfsDir> getSubdirs() throws IOException {
        // We use the tautologic filter, and no limit
        List<HdfsFile> lf = getListOfFiles();
        ArrayList<HdfsDir> result = new ArrayList<HdfsDir>();
        for (HdfsFile file : lf) {
            if (file.isDir()) {
                result.add((HdfsDir) file);
            }
        }
        return result;
    }

    /**
     * Get the list of files in the current directory
     *
     * @return List of paths to process
     * @throws IOException 
     */
    public List<HdfsFile> getListOfFiles() throws IOException {
        // We use the tautologic filter, and no limit
        return getListOfFiles(new TautologicFilter(), Integer.MAX_VALUE);
    }

    /**
     * Get the list of files in the current directory
     * @param pf A Path filter to retrieve only a subset of files
     * @param limit The maximal number of files to return
     * 
     * @return The list of filtered file paths
     * 
     * @throws IOException 
     */
    public List<HdfsFile> getListOfFiles(PathFilter pf,
            int limit) throws IOException {
        FileStatus[] listStatus = fs.listStatus(path, pf);
        ArrayList<HdfsFile> listOfFiles = new ArrayList<HdfsFile>();
        
        //Randomize order of files
        List<FileStatus> shuffledFiles = Arrays.asList(listStatus);
        Collections.shuffle(shuffledFiles);
        
        int counter = 0;
        for (FileStatus stat : shuffledFiles) {
            counter++;
            if (!stat.isDir()) {
                listOfFiles.add(new HdfsFile(fs, stat.getPath()));
            } else {
                listOfFiles.add(new HdfsDir(fs, stat.getPath()));
            }
            if (counter == limit) {
                break;
            }
        }
        return listOfFiles;
    }

    /**
     * Same as getListOfFiles and apply a PathFilter
     * 
     * @param filter Path filter
     * 
     * @return List of files
     * @throws IOException 
     */
    public List<HdfsFile> getListOfFiles(PathFilter filter) throws IOException {
        return getListOfFiles(filter, Integer.MAX_VALUE);
    }

    /**
     * Creating a sub-directory of the current dir
     *
     * Deprecated: better use createSubdir which is safer
     * 
     * @param child Name of the child directory
     * 
     * @return The new HdfsDir object
     */
    @Deprecated
    public HdfsDir createDir(String child) throws IOException {
        // TODO check that child is well formed as a dir. name
        Path newPath = new Path(path, child);
        fs.mkdirs(newPath);
        return new HdfsDir(fs, newPath);
    }

    /**
     * Creating a sub-directory of the current dir
     * 
     * @param child Name of the child directory
     * 
     * @return The new HdfsDir object
     */
    public HdfsDir createSubdir(String child) throws IOException {
        Path dirPath = new Path(path, child);
        try {
            // Check that the path does not exist
            if (!fs.exists(dirPath)) {
                fs.mkdirs(dirPath);
            } else {
                throw new IOException("Directory already exists: " + dirPath);
            }
            // Return the newly created dir
            return new HdfsDir(fs, dirPath);
        } catch (Exception ex) {
            throw new IOException("Unable to create directory " + dirPath.toString()
                    + ": " + ex.getMessage());
        }
    }
    
    /**
     * Create directory with current path, if it does not exists.
     * @return true if dir was created
     */
    public boolean create() {
        return createDir(fs, path);
    }
    
    /**
     * Create directory with current path, if it does not exists.
     * @return true if dir was created
     */
    public static boolean createDir(FileSystem fs, Path path) {
        boolean mkdirs = false;
        try {            
            if (!fs.exists(path)) {                
                mkdirs = fs.mkdirs(path);                
            }            
        } catch (IOException ex) {
            Logger.getLogger(HdfsDir.class.getName()).log(Level.SEVERE, null, ex);
        }
        return mkdirs;
    }

    /**
     * Creating a file in the curret directopry
     * 
     * @param fName Name of the new file
     * 
     * @return The new HdfsFile object
     */
    public HdfsFile createFile(String fName) throws IOException {

        // TODO check that fName is well formed
        Path newPath = new Path(path, fName);

        if (fs.exists(newPath)) {
            throw new IOException("File " + newPath + " already exists");
        }

        FSDataOutputStream os = fs.create(newPath);
        os.close();
        return new HdfsFile(fs, newPath);
    }

    /**
     * Delete the directory and all its content
     * 
     * 
     * @return true or false, depending on success / failure
     */
    @Override
    public boolean delete() throws IOException {
        if (isDir()) {
            List<HdfsFile> children = getListOfFiles();
            for (HdfsFile file : children) {
                boolean success = fs.delete(file.getPath(), true);
                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return fs.delete(path, true);
    }

    /**
     * Move a list of files in the current directory
     * 
     * @param listOfFiles A list of Path representing the files to move
     * 
     * @throws IOException 
     */
    public void moveFilesToDir(List<HdfsFile> listOfFiles) throws IOException {

        for (HdfsFile file : listOfFiles) {
            boolean rename = fs.rename(file.getPath(), new Path(path, file.getPath().getName()));
            System.out.println("Moved: " + rename + " - " + new Path(path, file.getPath().getName()));
        }
        // the filesystem must not be closed!! if closed, it closed all instances
        //fs.close();
    }

    /**
     * Loads the files from a Zip file into the current dir.
     * 
     * @param zipPath Name of the Zip file (on the local disk)
    
     */
    public void loadZipFile(String zipPath) {
        try {
            FileInputStream fis = new FileInputStream(zipPath);
            ZipInputStream zin = new ZipInputStream(new BufferedInputStream(fis));
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {

                if (entry.isDirectory()) {
                    // Assume directories are stored parents first then children.
                    (new File(entry.getName())).mkdir();
                    continue;
                }

                String fName = entry.getName();
                int dotPos = fName.lastIndexOf(".");
                int dotMacOS = fName.lastIndexOf(".DS_Store");
                if (dotMacOS == -1 && dotPos > 0 && fName.substring(dotPos) != "") {
                    System.out.println("Extracting file: " + fName);

                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                    // Seems that we need to read first the whole content of the entry
                    int nRead;
                    byte[] data = new byte[16384];
                    while ((nRead = zin.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    buffer.flush();
                    byte[] content = buffer.toByteArray();

                    // Create the HDFS file

                    HdfsFile hdfsFile = createFile(entry.getName());
                    // Append the content
                    hdfsFile.append(new ByteArrayInputStream(content));
                }

            }
        } catch (IOException ioe) {
            System.out.println("Exception when trying to process " + zipPath
                    + ": " + ioe.getMessage());
            ioe.printStackTrace();
            return;
        }

    }

    /**
     * Recursively set permission to current path and all sub-paths
     * @param permission
     * @throws IOException 
     */
    public void setPermissionsRecursive(FsPermission permission) throws IOException {
        // set permission for this dir
        setPermission(permission);

        List<HdfsFile> listOfFiles = getListOfFiles();
        List<HdfsDir> subdirs = getSubdirs();

        // set permission to all files
        for (HdfsFile file : listOfFiles) {
            file.setPermission(permission);
        }
        // go to all subdirs
        for (HdfsDir subdir : subdirs) {
            subdir.setPermissionsRecursive(permission);
        }
    }
    
    /**
     * Recursively set replication factor to current path and all sub-paths
     * 
     * @param replication The replication factor
     * @return true if all path and sub-paths set replication succeeded
     * 
     * @throws IOException 
     */
    public boolean setReplicationRecursive(short replication) throws IOException {
        // set permission for this dir
        boolean setReplication = setReplication(replication);

        List<HdfsFile> listOfFiles = getListOfFiles();
        List<HdfsDir> subdirs = getSubdirs();

        // set permission to all files
        for (HdfsFile file : listOfFiles) {
            boolean fileSet = file.setReplication(replication);
            if (!fileSet)
                setReplication = false;
        }
        // go to all subdirs
        for (HdfsDir subdir : subdirs) {
            boolean subDirSet = subdir.setReplicationRecursive(replication);
            if (!subDirSet)
                setReplication = false;
        }
        return setReplication;
    }


//    /**
//     * The size of a directory as the sum of the files sizes it contains
//     */
//    public long getFlatSize() throws IOException {
//
// 	   long readCount = 0L;
//       long size = 0L;
//       
// 	   RemoteIterator<LocatedFileStatus> i = fs.listLocatedStatus(path);
// 	   while(i.hasNext()){
// 		   readCount++;
// 		   if(readCount % 10000 == 0)
// 			   System.out.println("Get flat size: " + readCount + " LocatedFileStatus were read.");
// 		   
// 		   LocatedFileStatus status = i.next();
// 		   if(status.isFile()){
// 			   size += status.getLen();
// 		   }
// 	   }
//
//        return size;
//    } 
    	
    /**
    * Nb of files in a directory
    */
   public long getNbFiles() throws IOException {
	   
	   ContentSummary cs = fs.getContentSummary(path);
	   return cs.getFileCount();

   }
   
    public static List<Path> getListOfFilePaths(Path directory, PathFilter pf,
    		FileSystem fs) throws FileNotFoundException, IOException {
    	FileStatus[] listStatus = fs.listStatus(directory, pf);
    	
    	List<Path> files = new ArrayList<Path>(listStatus.length);
    	for (int i = 0; i < listStatus.length; i++) {
			if(!listStatus[i].isDir()) {
				files.add(listStatus[i].getPath());
			}
		}
    	
    	return files;
    }
}
