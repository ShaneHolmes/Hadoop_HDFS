package com.shane.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase的API，DDL
 * 数据定义语言的API操作，通过admin对象
 */
public class HBaseAPIDDL {
    /**
     * API的使用
     * 第一步：获得配置文件
     * 第二步：创建连接对象
     * 第三步：创建HBaseAdmin对象（管理、访问表需要通过admin对象）
     * 第四步：通过admin进行操作
     */
    public static Configuration conf = null;
    public static Connection conn = null;
    public static Admin admin = null;
    //静态代码块设置配置文件信息、获得连接、获得hbaseadmin对象
    static {
        //创建hbase配置文件并设置
        conf = HBaseConfiguration.create();//单例方法
        conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03,hadoop04");
        try {
            //传入配置文件创建连接
            conn = ConnectionFactory.createConnection(conf);
            //通过连接得到的admin是一个hbaseAdmin对象.关闭通过close方法。
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {

        String ns = "myhbase";
        String tn = ns+":mystudent";
        String[] cf = {"info1","info2"};

        createNameSpace(ns);

        if(!isTableExist(tn)){
            createTable(tn,cf);
        }

        deleteTable(tn);

        deleteNameSpace(ns);

        //最后关闭资源
        close();
    }

    /**
     * 判断表是否存在
     */

    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     *创建表
     */
    public static void createTable(String tableName, String... columnFamilies) throws IOException {//可变形参
        if(isTableExist(tableName)){//判断表是否存在
            System.out.println("errorInfo：表" + tableName +" 已经存在");
        }else {
            //判断列族参数是否为空
            if(columnFamilies.length <= 0){
                System.out.println("errorInfo：需要至少一个列族");
            }else {//合法，可以创建
                //创建表描述器
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
                //添加列族
                for(String cf : columnFamilies){
                    descriptor.addFamily(new HColumnDescriptor(cf));
                }
                //创建表
                admin.createTable(descriptor);
                System.out.println("info：表"+tableName+"创建成功");
            }
        }
    }

    /**
     *删除表
     */
    public static void deleteTable(String tableName) throws IOException {
        if(!isTableExist(tableName)){
            System.out.println("errorInfo：表" + tableName +" 不存在");
        }else {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("successInfo：表" + tableName +"删除成功");
        }
    }

    /**
     * 创建命名空间*/
    public static void createNameSpace(String nameSpace){
        //创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e){
            System.out.println("errorInfo：命名空间"+nameSpace+"已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //在命名空间已经存在的情况下，try...catch代码块后面的代码依然能够执行
        //但是如果过使用方法throws IOException的方式，方法抛出异常之后，异常后面的代码不会被执行,main函数中调用此方法的后面的代码不会被执行
        //System.out.println("try...catch后能够执行的代码段");
        System.out.println("info：命名空间创建成功");
    }

    /**
     * 删除命名空间
     */
    public static void deleteNameSpace(String nameSpace) throws IOException {
        //nameSpace下的所有表
        TableName[] tableNames = admin.listTableNamesByNamespace(nameSpace);
        if(tableNames.length != 0){
            System.out.println("errorInfo:命名空间"+nameSpace+"下的还有表，删除失败");
            System.out.println("表名为：");
            for (TableName tableName : tableNames) {
                System.out.print(tableName+"\t");
            }
        }
        admin.deleteNamespace(nameSpace);
        System.out.println("successInfo：命名空间" + nameSpace +"删除成功");
    }

    /**
     * 关闭hbaseadmin,关闭connection
     */
    public static void close() throws IOException {
        if(admin != null){
            admin.close();
        }

        if(conn != null){
            conn.close();
        }
    }


}
