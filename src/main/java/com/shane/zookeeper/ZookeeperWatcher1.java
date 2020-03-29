package com.shane.zookeeper;

import org.apache.zookeeper.*;

/**
 * 实现监听有三种方式，这是第一种
 * 这种方式process调用两次：在节点添加监听的时候一次，第一次create时触发监听一次
 */

public class ZookeeperWatcher1 {
    private static final String connect = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int sessionTimeout = 4000;

    public static void main(String[] args) throws Exception{

        //创建zookeeper对象，并创建一个Watcher去监听
        ZooKeeper zooKeeper = new ZooKeeper(connect, sessionTimeout, new Watcher() {
            //事件触发响应的方法，在process方法里面进行响应的逻辑的处理
            @Override
            public void process(WatchedEvent watchedEvent) {
                //获取事件类型
                Event.EventType eventType = watchedEvent.getType();
                //事件的路径
                String path = watchedEvent.getPath();
                //事件的状态
                Event.KeeperState state = watchedEvent.getState();

                /**
                 *  第一： 如果 state 有值(SyncConnected=3)， 但是 type 和  path无值， 那就证明是在获取连接
                 *
                 *  第二： type和path的意义就是告诉 监听器的 process 方法
                 *  到底是哪个 path的那个事件被响应了。 以便process方法能够调用对应的业务逻辑的代码执行
                 */
                System.out.println(path + "\t" + eventType  +  "\t" + state);

                if(path == null && state.getIntValue() == 3) {
                    System.out.println("connect success!");
                }else {
                    System.out.println(path + "节点的" + eventType + "事件被触发了");
                }
            }
        });

        //在节点/mydata上添加监听,true代表开启监听,触发的监听由Watcher.添加时触发执行process
        zooKeeper.getChildren("/mydata",true);

        //触发监听，执行process
        zooKeeper.create("/mydata/node","node_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
        //监听只会响应一次，第二次触发监听时不会得到响应
        zooKeeper.create("/mydata/node","node_data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);

        zooKeeper.close();
    }
}


