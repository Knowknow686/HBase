package com.class_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


/*该类用于向HBase中的指定表插入数据*/
public class InsertData {
    /*主方法，程序入口点
     @param args 命令行参数*/
    public static void main(String[] args) {
        // 创建HBase配置对象
        Configuration config = HBaseConfiguration.create();
        //指定zookeeper集群地址
        config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("student"))) {
            Put put = new Put(Bytes.toBytes("001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
            put.addColumn(Bytes.toBytes("score"), Bytes.toBytes("math"), Bytes.toBytes("90"));
            table.put(put);
            System.out.println("Insert data successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}