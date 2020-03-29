package com.shane.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 实现两个功能：服务器上下线
 *
 * 利用zookeeper中znode临时节点的特性
 * 1.创建一个/servers节点下的临时子节点来模拟一个datanode的上线
 * 2.删除一个/servers节点下的临时子节点来模拟一个datanode的下线
 *
 * 某datanode上线后在/servers节点下的临时子节点，在运行期间会一直维护这个会话，这个临时节点一直
 * 存在。宕机后，会话断开，zookeeper会自动删除这个临时节点。无论是上下线，/servers的子节点发生了
 * 变化，在该节点注册监听的namenode被触发响应。达到集群感知的效果
 *
 */
public class ZooKeeperServersUpDown {
    private static final String CONNECT = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
    private static final int SESSION_TIMEOUT = 4000;
    private static final String SERVERS_NODE = "/servers";

    public static void main(String[] args) throws Exception{
        ZooKeeper zooKeeper = new ZooKeeper(CONNECT,SESSION_TIMEOUT,null);

        //判断/servers是否存在
        Stat exists = zooKeeper.exists(SERVERS_NODE, null);
        if(exists == null){
            zooKeeper.create(SERVERS_NODE,"SERVERS".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //hadoop01上线
        zooKeeper.create(SERVERS_NODE+"/hadoop02","hadoop01_server".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);

        Thread.sleep(Long.MAX_VALUE);

        zooKeeper.close();
    }
}
