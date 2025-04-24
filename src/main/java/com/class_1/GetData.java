package com.class_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/*该类用于向HBase中的指定表中行键查询数据*/
public class GetData {
    public static void main(String[] args) {
        // 创建HBase配置对象
        Configuration config = HBaseConfiguration.create();
        //指定zookeeper集群地址
        config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("student"))) {
            // 创建Get对象以查询指定行键的数据
            Get get = new Get(Bytes.toBytes("001"));
            // 执行查询并获取结果
            Result result = table.get(get);
            // 从结果中提取name、age和mathScore列的值
            byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
            byte[] mathScore = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("math"));
            // 打印查询结果
            System.out.println("Name: " + Bytes.toString(name));
            System.out.println("Age: " + Bytes.toString(age));
            System.out.println("Math Score: " + Bytes.toString(mathScore));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
