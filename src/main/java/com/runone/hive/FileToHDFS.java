package com.runone.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class FileToHDFS {

    private FileSystem fileSystem = null;

    private Properties hdfsprop;

    private Configuration conf;

    public FileToHDFS(Properties hdfsprop) {
        this.hdfsprop = hdfsprop;
        this.loadConfig();
    }

    private void loadConfig() {
        Configuration conf = new Configuration();
        System.out.println("hdfs.properties:" + hdfsprop);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.support.append", "true");
        //指定文件块的个数，默认是3个，太多了会影响效率。
        conf.set("dfs.replication", this.hdfsprop.getProperty("data.block.replica", "2"));
        //文件不能太大，避免数据不够造成大量的浪费，合理利用文件块
        conf.set("dfs.blocksize", this.hdfsprop.getProperty("data.split.blocksize", "128m"));
        this.conf = conf;
    }


    public FSDataOutputStream getOutputSteamFromHDFS(String filePath) {

        Logger logger = LoggerFactory.getLogger(FileToHDFS.class);

        FSDataOutputStream out = null;

        try {
            fileSystem = FileSystem.get(new URI(filePath), conf);
            Path path = new Path(filePath);
            if (!fileSystem.exists(path)) {
                logger.info("the file in your path doesn't exists,prepare to create one...");
                out = fileSystem.create(path, false);
            } else {
                out = fileSystem.append(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return out;
    }

    public void close() {
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
