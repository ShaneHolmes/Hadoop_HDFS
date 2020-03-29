package com.shane.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 集群配置管理
 * Client模拟修改配置信息，实现增加、修改和删除配置
 */
public class ZooKeeperConfClient {
    public static final String CONNECT = "hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181";
    public static final int SESSION_TIMEOUT = 4000;
    public static final String CONFIGURATIONS = "/configurations";

    public static void main(String[] args) throws Exception{
        ZooKeeper zooKeeper = new ZooKeeper(CONNECT,SESSION_TIMEOUT,null);

        //判断/configurations是否存在，不存在创建
        Stat exists = zooKeeper.exists(CONFIGURATIONS, null);
        if (exists == null) {
            zooKeeper.create(CONFIGURATIONS,"configurations".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String key = "conf1",value = "{name:shane home:Haikou}";//key文件名，value文件内容
        //模拟增加配置信息
//
//        zooKeeper.create(CONFIGURATIONS+"/"+key,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
//        key = "conf2";
//        value = "{name:shane home:Sanya}";
//        zooKeeper.create(CONFIGURATIONS+"/"+key,value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        //模拟修改配置信息
//        key = "conf2";
//        value = "{name:shane home:Shenzhen1}";
//        zooKeeper.setData(CONFIGURATIONS+"/"+key,value.getBytes(),-1);
//        //模拟删除配置
          zooKeeper.delete(CONFIGURATIONS+"/"+key,-1);

        zooKeeper.close();
    }

}
