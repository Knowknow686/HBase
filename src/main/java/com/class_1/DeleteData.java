package com.class_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/*该类用于向HBase中的指定表中行键删除数据*/
public class DeleteData {
    /*主方法，程序入口*/
    public static void main(String[] args) {
        // 创建HBase配置对象
        Configuration config = HBaseConfiguration.create();
        // 指定zookeeper集群地址
        config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("student"))) {
            Delete delete = new Delete(Bytes.toBytes("001"));
            table.delete(delete);
            System.out.println("Delete data successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
