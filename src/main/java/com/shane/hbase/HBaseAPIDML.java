package com.shane.hbase;

import groovy.json.internal.Byt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.hbase.HBaseUtils;

import java.io.IOException;

/*
* HBase API的DML
* 数据操纵语言，通过table/hTable对象
* */
public class HBaseAPIDML{
    public static Configuration conf = null;
    public static Connection conn = null;
    public static Table table = null;
    //静态代码块设置配置文件信息、获得连接
    //但是，获得table对象需要传入tablename，所有的DML操作连接的zk的配置和连接用的同一个可拿出来
    //但tableName不同，得到的table对象不同，因此在具体的方法中获得table对象
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03,hadoop04");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String ns = "myhbase";
        String tn = ns+":mystudent";
        String rk = null;
        String[] cf = {"info1","info2"};

        //创建命名空间和表
        HBaseAPIDDL.createNameSpace(ns);
        HBaseAPIDDL.createTable(tn,cf);

        //插入数据rk = "1001";
        rk = "1001";
        putIntoTable(tn, rk, cf[0], "name", "shaneholmes");
        putIntoTable(tn, rk, cf[0], "name", "shane");
        putIntoTable(tn, rk, cf[0], "name", "holmes");
        putIntoTable(tn, rk, cf[0], "addr", "haikou");
        putIntoTable(tn, rk, cf[1], "tel", "17776991455");
        putIntoTable(tn, rk, cf[1], "github", "shaneholmes");
        rk = "1002";
        putIntoTable(tn, rk, cf[0], "name", "shaneholmes");
        putIntoTable(tn, rk, cf[0], "age", "18");
        putIntoTable(tn, rk, cf[1], "tel", "17776991455");
        putIntoTable(tn, rk, cf[1], "github", "shaneholmes");

        //get读取数据
        getFromTable(tn,"1001",null,null);//读rk="1001"的数据
        getFromTable(tn,"1001",cf[0],"name");//读某一cell的数据，能读多版本.需要设置？？？

        //scan读取数据
        scanFromTable(tn, cf[0], "name");
        scanFromTable(tn, cf[0], null);
        scanFromTable(tn, null, null);

        //删除数据
        deleteFromTable(tn,"1001",null,null);
        scanFromTable(tn, null, null);

        //删除表和命名空间
        HBaseAPIDDL.deleteTable(tn);
        HBaseAPIDDL.deleteNameSpace(ns);

        //最后需要将资源关闭
        close();
    }


    /**
     *往表中插入数据
     */
    public static void putIntoTable(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        //每一个DML操作都需要传入表名获取table对象
        //表名可能不同，所以需要再具体的方法中传入tableName获得table对象
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        /*
        * 写代码的思路：
        * 在获得table对象后，我们知道需要用table对象的put方法，写出 table.put();发现需要传入一个Put对象
        * 于是new Put得到put，同样的Put需要传入rowKey，但是是字节数组的形式传入（hbase存的全是字节数组）
        * 那么还有cf，c，v等参数？
        * 通过addColumn方法添加到参数为rowKey的put对象中
        * 最后将put传入table.put(put);即可
        * */

        //需要将table对象关闭
        System.out.println("successInfo：" + tableName +"\t"+rowKey+"\t"+columnFamily+"\t"+column+"\t"+"写入成功");
        table.close();
    }

    /**
     * get方式读取表中的数据.tableName&rowKey是必选项，cf&c可为空
     */
    public static void getFromTable(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //获取数据的版本数,需4个参数指定cell
        get.setMaxVersions(2);
        if(columnFamily != null && column != null){
            //指定列族和列
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        }else if (columnFamily != null){
            //指定列族
            get.addFamily(Bytes.toBytes(columnFamily));
        }
        Result result = table.get(get);

        System.out.println("Get表："+tableName+",键值："+rowKey+",列族："+columnFamily+",列："+column+" 的结果是：");
        for (Cell cell : result.rawCells()) {
            System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+
                    "\tcolumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                    "\tcolumn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    "\t value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    /**
     *scan方式获取表中的数据,columnFamily&column是可选参数
     */
    public static void scanFromTable(String tableName, String columnFamily, String column) throws IOException {
        //也可以通过HTable对象来操作表中的数据
        HTable hTable = (HTable) conn.getTable(TableName.valueOf(tableName));
        
        ResultScanner scanner =null;//scanner是多行结果数据，每一行有多个cell
        if(columnFamily != null && column != null){//都不为空是scan某一column
            scanner = hTable.getScanner(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        }else if(columnFamily != null ){//scan某一列族
            scanner = hTable.getScanner(Bytes.toBytes(columnFamily));
        }else {//扫描全表
            Scan scan = new Scan();
            scanner = hTable.getScanner(scan);
        }

        System.out.println("Scan表："+tableName+",列族："+columnFamily+",列："+column+" 的结果是：");
        for (Result result : scanner) {
            //scanner是多行结果数据,result是一行，每一行被唯一的rowKey标识，有多个cell.
            //一个cell在scan的时候就是一条数据
            for (Cell cell : result.rawCells()) {
                System.out.println("tableName:"+tableName+
                        "\trowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+
                        "\tcolumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                        "\tcolumn:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "\t value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }

    /**
     *删除表中的数据,
     */
    private static void deleteFromTable(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        HTable hTable = (HTable) conn.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        if(columnFamily != null && column != null){
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        }

        hTable.delete(delete);
        System.out.println("successInfo：表："+tableName+",列族："+rowKey+",列族："+columnFamily+",列："+column+" 删除成功");
        hTable.close();
    }

    /**
     *关闭连接的资源
     */
    public static void close() throws IOException {
        conn.close();
    }
}
