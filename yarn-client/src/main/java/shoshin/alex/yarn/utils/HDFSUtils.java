package shoshin.alex.yarn.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSUtils {
    private FileSystem fs;
    
    public HDFSUtils(Configuration conf) throws IOException {
        fs = FileSystem.get(conf);
    }
    
    public FileStatus copyToHDFS(String localPath, String destPath) throws IOException {
        Path path = new Path(fs.getHomeDirectory(), destPath);
        fs.copyFromLocalFile(new Path(localPath), path);
        return fs.getFileStatus(path);
    }
}