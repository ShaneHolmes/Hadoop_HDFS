package com.shane.zookeeper;

import org.apache.zookeeper.*;

/**
 * 实现监听有三种方式，这是第二种
 * 这种方式process也是调用两次：在节点添加监听的时候一次，第一次create时触发监听一次
 */

public class ZooKeeperWatcher2 {
    private static final String connect = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int sessionTimeout = 4000;

    public static void main(String[] args) throws Exception{

        //创建zookeeper对象，watcher为自定义ZooKeeperWatcherCommon
        ZooKeeper zooKeeper = new ZooKeeper(connect, sessionTimeout, new ZooKeeperWatcherCommon());

        //在节点/mydata上添加监听，并创建自己的一个监听器对象new ZooKeeperWatcherCommon()
        zooKeeper.getChildren("/mydata",true);

        //触发监听，执行process
        zooKeeper.create("/mydata/node","node_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
        //监听只会响应一次，第二次触发监听时不会得到响应
        zooKeeper.create("/mydata/node","node_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);

        zooKeeper.close();

    }
}


