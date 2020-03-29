package com.shane.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 服务器动态上下线感知
 *
 * 需求背景：HDFS中的datanode宕机之后，namenode需要经过10:30时间才能感知
 * 需求：如果HDFS集群中的任何一台服务器上线或者下线，namenode都能够做到实时的感知到
 *
 * 当前的namenode一直运行着，一直监听/servers节点（循环监听）
 */
public class ZooKeeperServersNamenode {
    private static final String CONNECT = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int SESSION_TIMEOUT = 4000;
    private static final String SERVERS_NODE = "/servers";
    static ZooKeeper zooKeeper = null;
    static List<String> oldChildNodeList;

    /**
     *从两个孩子列表中，获取/servers的子节点中改变的那一个：增加或减少
     */
    public static String getServerFromDiffBetweenTwoList(List<String> oldChildNodeList,List<String> newChildNodeList){
        Boolean flag = true;//默认是上线，即子节点新增
        if(oldChildNodeList.size() > newChildNodeList.size()){
            flag = false;//下线，子节点减少
        }


        if(flag){//上线，节点增加，new的多
            for(String newChildNode : newChildNodeList){
                if(!oldChildNodeList.contains(newChildNode)){//旧的列表不包含新列表的某一项
                    return newChildNode;
                }
            }
        }else {
            for(String oldChildNode : oldChildNodeList){
                if(!newChildNodeList.contains(oldChildNode)){
                    return oldChildNode;
                }
            }
        }

        return null;

    }

    public static void main(String[] args) throws Exception{
        zooKeeper = new ZooKeeper(CONNECT, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                Event.EventType eventType = watchedEvent.getType();
                String path = watchedEvent.getPath();
                Event.KeeperState state = watchedEvent.getState();

                //System.out.println(path + "\t" + eventType  +  "\t" + state);

                if(path != null && state.getIntValue() == 3) {
                    //获取新子节点列表并重新监听
                    List<String> newChildNodeList = null;
                    try {
                       newChildNodeList = zooKeeper.getChildren(SERVERS_NODE,true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if(eventType == Event.EventType.NodeChildrenChanged){
                        String upOrDown = newChildNodeList.size() > oldChildNodeList.size() ? "上线" : "下线";
                        String server = getServerFromDiffBetweenTwoList(oldChildNodeList,newChildNodeList);
                        System.out.println(upOrDown+"了一台服务器: "+server);

                        //更新oldChildNodeList
                        oldChildNodeList = newChildNodeList;
                    }
                }


            }
        });

        //判断/servers是否存在
        Stat exists = zooKeeper.exists(SERVERS_NODE, null);
        if(exists == null){
            zooKeeper.create(SERVERS_NODE,"SERVERS".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //添加NodeChildrenChanged事件的监听
        oldChildNodeList = zooKeeper.getChildren(SERVERS_NODE,true);

        //当前进程一直运行
        Thread.sleep(Long.MAX_VALUE);

        zooKeeper.close();
    }
}
