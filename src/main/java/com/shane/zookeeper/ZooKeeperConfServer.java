package com.shane.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ZooKeeperConfServer {
    public static final String CONNECT = "hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181";
    public static final int SESSION_TIMEOUT = 4000;
    public static final String CONFIGURATIONS = "/configurations";
    static ZooKeeper zooKeeper = null;
    static List<String> oldChildNodeList = null;

    /**
     *从两个孩子列表中，获取/servers的子节点中改变的那一个：增加或减少
     */
    public static String getNewServer(List<String> oldChildNodeList, List<String> newChildNodeList) {
        for (String newChildNode : newChildNodeList) {
            if (!oldChildNodeList.contains(newChildNode)) {//旧的列表不包含新列表的某一项
                return newChildNode;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception{
        zooKeeper = new ZooKeeper(CONNECT, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                Event.EventType type = watchedEvent.getType();     //NodeChildrenChanged、NodeDataChanged、Nodedeleted
                String path = watchedEvent.getPath();              //CONFIGURATIONS、CONFIGURATIONS+子节点
                Event.KeeperState state = watchedEvent.getState();

                if(path != null && state.getIntValue() == 3){//屏蔽连接

                    List<String> newChildNodeList = null;
                    try {
                        newChildNodeList = zooKeeper.getChildren(CONFIGURATIONS,true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(path.equals(CONFIGURATIONS) && type == Event.EventType.NodeChildrenChanged){
                        //这里处理增加
                        if(oldChildNodeList.size()<newChildNodeList.size()){
                            String newServer = getNewServer(oldChildNodeList,newChildNodeList);
                            byte[] data = null;
                            try {
                                data = zooKeeper.getData(CONFIGURATIONS + "/" + newServer, null, null);
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            System.out.println("新增配置:\n"+"名称："+newServer+"\t内容："+new String(data));
                        }

                    }else{
                        for(String child : oldChildNodeList){
                            String childPath = CONFIGURATIONS+"/"+child;
                            String delOrModServer = getNewServer(newChildNodeList,oldChildNodeList);

                            //这里处理删除
                            if (path.equals(childPath) && type == Event.EventType.NodeDeleted) {
                                System.out.println("删除配置:\n"+"名称："+child);
                            }

                            //这里处理修改
                            if (path.equals(childPath) && type == Event.EventType.NodeDataChanged) {
                                byte[] data = new byte[0];
                                try {
                                    data = zooKeeper.getData(childPath,true, null);
                                } catch (KeeperException e) {
                                    e.printStackTrace();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                System.out.println("修改配置:\n"+"名称："+child+"\t新内容："+new String(data));
                            }
                        }
                    }
                    oldChildNodeList = newChildNodeList;
                }
            }
        });

        //判断/configurations是否存在，不存在创建
        Stat exists = zooKeeper.exists(CONFIGURATIONS, null);
        if (exists == null) {
            zooKeeper.create(CONFIGURATIONS,"configurations".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        /**
         * 添加监听:
         * 1.给/configurations添加NodeChildrenChanged监听（增加和删除）
         * 2.给/configurations的子节点添加NodeDataChanged监听（修改）
         */
        oldChildNodeList = zooKeeper.getChildren(CONFIGURATIONS, true);
        for(String child : oldChildNodeList){
            String childPath = CONFIGURATIONS+"/"+child;
            zooKeeper.getData(childPath,true,null);
        }

        //有两个线程：main主线程和watcher线程。主线程不结束（休眠时）监听线程一直存在，等待被触发
        Thread.sleep(Long.MAX_VALUE);

        zooKeeper.close();
    }
}
