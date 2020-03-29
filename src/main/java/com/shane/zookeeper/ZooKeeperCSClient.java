package com.shane.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 客户端不需要添加监听器
 */
public class ZooKeeperCSClient {
    private static final String connect = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int sessionTimeout = 4000;

    public static void main(String[] args) throws Exception{
        ZooKeeper zooKeeper = new ZooKeeper(connect,sessionTimeout,null);
        //创建节点触发server监听响应
        zooKeeper.create("/mydata/node","node_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE
        , CreateMode.PERSISTENT_SEQUENTIAL);
        zooKeeper.close();
    }
}
