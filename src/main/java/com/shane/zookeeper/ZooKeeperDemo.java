package com.shane.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * 操作zk znode
 */

public class ZooKeeperDemo {

    public static void main(String[] args) throws Exception{
        //请求连接的服务器地址
        String connect = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //连接超时时长
        int sessionTimeout = 4000;
        //znode节点名称
        String znode = "/zk/znode1";



        //创建zk对象
        ZooKeeper zk = new ZooKeeper(connect,sessionTimeout,null);

        //创建znode节点
        String createNNode = zk.create(znode,"znode's data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //判断节点是否存在
        Stat stat = zk.exists(znode,null);
        Boolean exists = stat != null ? true : false;
        System.out.println("节点是否存在？ "+exists);

        //查看节点
        byte[] data = zk.getData(znode,null,null);
        System.out.println("节点的信息： "+new String(data));

        //修改节点数据
        Stat setData = zk.setData(znode,"modified data".getBytes(),-1);

        //查看修改后的节点
        byte[] modifiedData = zk.getData(znode,null,null);
        System.out.println("修改后节点的信息： "+new String(modifiedData));

        //获取znode的权限信息并打印
        List<ACL> aclList = zk.getACL(znode,null);
        System.out.println("权限信息：");
        for(ACL acl : aclList){
            System.out.print(acl.getPerms()+"\t");
        }
        System.out.println();

        //获取子节点
        List<String> children = zk.getChildren("/",null);
        System.out.println("/的子节点： ");
        for(String child : children){
            System.out.print(child+"\t");
        }
        System.out.println();

        //删除节点
        zk.delete(znode,-1);

        //判断节点是否存在
        Stat stat1 = zk.exists(znode,null);
        Boolean existsAfterDelete = stat1 != null ? true : false;
        System.out.println("删除节点后节点是否存在？ "+existsAfterDelete);

        //将zk连接关闭
        zk.close();
    }
}
