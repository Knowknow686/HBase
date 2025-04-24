package com.class_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/*查询HBase中student表中的所有数据*/
public class ScanData {
    // 主函数，程序入口
    public static void main(String[] args) {
        // 创建HBase配置对象并设置zookeeper集群地址
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("student"))) {
            // 扫描student表并打印所有数据的RowKey和Name列
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] rowKey = result.getRow();
                byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
                System.out.println("RowKey: " + Bytes.toString(rowKey) + ", Name: " + Bytes.toString(name));
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
