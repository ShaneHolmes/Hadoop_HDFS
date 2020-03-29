package com.shane.zookeeper;

import org.apache.zookeeper.*;

/**
 * 循环监听的Server
 */
public class ZooKeeperCSServer {
    private static final String connect = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int sessionTimeout = 4000;

    static ZooKeeper zooKeeper =null;

    public static void main(String[] args) throws Exception{
        //创建zookeeper对象，并创建一个Watcher去监听
        zooKeeper = new ZooKeeper(connect, sessionTimeout, new Watcher() {
            //事件触发响应的方法，在process方法里面进行响应的逻辑的处理
            @Override
            public void process(WatchedEvent watchedEvent) {
                Event.EventType eventType = watchedEvent.getType();
                String path = watchedEvent.getPath();
                Event.KeeperState state = watchedEvent.getState();

                System.out.println(path + "\t" + eventType  +  "\t" + state);

                if(path == null && state.getIntValue() == 3) {
                    System.out.println("connect success!");
                }else {
                    System.out.println(path + "节点的" + eventType + "事件被触发了");
                }

                //由于监听只能响应一次，响应完后需要再次添加监听
                try {
                    zooKeeper.getChildren("/mydata",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        //在节点/mydata上添加监听,true代表开启监听
        zooKeeper.getChildren("/mydata",true);

        //让服务器一直监听，需要使这个服务器进程不掉
        Thread.sleep(Long.MAX_VALUE);


        zooKeeper.close();
    }
}
