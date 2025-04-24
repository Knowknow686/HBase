package com.class_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

// 创建HBase表的主类
public class CreateTable {
    public static void main(String[] args) {
        // 创建HBase配置对象
        Configuration config = HBaseConfiguration.create();
        // 指定zookeeper集群地址
        config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()){
            // 定义表名和表描述符
            TableName tableName = TableName.valueOf("student");
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            // 添加列族到表描述符
            HColumnDescriptor infoColumnFamily = new HColumnDescriptor("info");
            HColumnDescriptor scoreColumnFamily = new HColumnDescriptor("score");
            tableDescriptor.addFamily(infoColumnFamily);
            tableDescriptor.addFamily(scoreColumnFamily);
            // 检查表是否存在，如果不存在则创建表
            if (!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
                System.out.println("Table already create");
            } else{
                System.out.println("Table already exists");
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}