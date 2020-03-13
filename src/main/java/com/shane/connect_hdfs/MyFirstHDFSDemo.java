package com.shane.connect_hdfs;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyFirstHDFSDemo {
    public static void main(String[] args) throws Exception {
        URI uri = new URI("hdfs://192.168.65.101:9000/");
        Configuration conf = new Configuration();
        String user = "hadoop";
        FileSystem fs = FileSystem.get(uri,conf,user);

        fs.copyFromLocalFile(new Path("C:\\Users\\47463\\Desktop\\temp\\test.txt"), new Path("/"));
        //fs.copyFromLocalFile(new Path(args[0]), new Path(args[1]));
        boolean exist = fs.exists(new Path("/test.txt"));
        if(exist) {
            System.out.println("success");
        }else {
            System.out.println("failed");
        }
        fs.close();
    }
}