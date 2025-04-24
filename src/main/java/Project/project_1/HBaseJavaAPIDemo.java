package Project.project_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseJavaAPIDemo {
    // 定义表名
    private static final String Table_Name = "employee";
    // 定义列族名info
    private static final String Column_Family_Info = "info";
    // 定义列族名dept
    private static final String Column_Family_Dept = "dept";

    /*创建表
    * @param admin HBase管理员对象
    * @throws IOExceptio*/
    public static void createTable(Admin admin) throws Exception {
        // 将表名转换为TableName对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 检查表是否存在
        if (!admin.tableExists(tableName)) {
            // 创建表描述符构建器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            // 创建列族info描述符
            ColumnFamilyDescriptor InfoFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(Column_Family_Info)).build();
            // 创建列族dept描述符
            ColumnFamilyDescriptor DeptFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(Column_Family_Dept)).build();
            // 设置列族info到表描述符构建器
            tableDescriptorBuilder.setColumnFamily(InfoFamily);
            // 设置列族dept到表描述符构建器
            tableDescriptorBuilder.setColumnFamily(DeptFamily);
            // 构建表描述符
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            // 使用admin对象创建表
            admin.createTable(tableDescriptor);
            // 输出创建成功的消息
            System.out.println("Table created successfully.");
        } else {
            // 如果表已存在，则输出提示信息
            System.out.println("Table already exists.");
        }
    }

    /*插入数据
    * @param connection HBase连接对象
    * @throws IOException*/
    public static void insertData(Connection connection) throws IOException {
        // 获取表的连接对象
        try (Table table = connection.getTable(TableName.valueOf(Table_Name))) {
            // 创建Put对象，准备插入数据，指定rowKey为"emp1"
            Put put = new Put(Bytes.toBytes("emp1"));
            // 向info列族的name列插入数据"John Doe"
            put.addColumn(Bytes.toBytes(Column_Family_Info), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            // 向info列族的age列插入数据"30"
            put.addColumn(Bytes.toBytes(Column_Family_Info), Bytes.toBytes("age"), Bytes.toBytes("30"));
            // 向dept列族的department列插入数据"Engineering"
            put.addColumn(Bytes.toBytes(Column_Family_Dept), Bytes.toBytes("department"), Bytes.toBytes("Engineering"));
            // 将put对象插入到表中
            table.put(put);
            // 输出插入成功的消息
            System.out.println("Data inserted successfully.");
        } catch (IOException e) {
            // 如果插入过程中发生错误，输出错误信息
            System.err.println("Error inserting data: " + e.getMessage());
            // 抛出异常
            throw e;
        }
    }

    /*获取单条数据
    * @param connection HBase连接对象
    * @parm rowKey 行键
    * @throws IOException 操作异常*/
    public static void getData(Connection connection, String rowKey) throws IOException {
        // 获取表的连接对象
        try (Table table = connection.getTable(TableName.valueOf(Table_Name))) {
            // 创建Get对象，指定要获取的rowKey
            Get get = new Get(Bytes.toBytes(rowKey));
            // 执行get操作获取数据
            Result result = table.get(get);
            // 检查结果是否为空
            if (!result.isEmpty()) {
                // 获取info列族name列的数据
                byte[] name = result.getValue(Bytes.toBytes(Column_Family_Info), Bytes.toBytes("name"));
                // 获取info列族age列的数据
                byte[] age = result.getValue(Bytes.toBytes(Column_Family_Info), Bytes.toBytes("age"));
                // 获取dept列族department列的数据
                byte[] dept = result.getValue(Bytes.toBytes(Column_Family_Dept), Bytes.toBytes("department"));
                System.out.println("Row:" + rowKey);
                System.out.println("Name: " + Bytes.toString(name));
                System.out.println("Age: " + Bytes.toString(age));
                System.out.println("Department: " + Bytes.toString(dept));
            } else {
                // 如果结果为空，则输出提示信息
                System.out.println("No data found for rowkey: " + rowKey);
            }
        } catch (IOException e) {
            // 如果获取过程中发生错误，输出错误信息
            System.err.println("Error retrieving data: " + e.getMessage());
            // 抛出异常
            throw e;
        }
    }

    /*扫描数据
    * @param connection HBase连接对象
    * @throws IOException 操作异常*/
    public static void ScanData(Connection connection) throws IOException {
        // 获取表的连接对象
        try (Table table = connection.getTable(TableName.valueOf(Table_Name))) {
            // 创建扫描器，扫描所有数据
            ResultScanner scanner = table.getScanner(new Scan());
            // 遍历扫描结果
            for (Result result : scanner) {
                // 获取每一行的rowKey
                byte[] row = result.getRow();
                // 获取info列族name列的数据
                byte[] name = result.getValue(Bytes.toBytes(Column_Family_Info), Bytes.toBytes("name"));
                // 输出rowKey和name列的数据
                System.out.println("Row:" + Bytes.toString(row));
                System.out.println("Name: " + Bytes.toString(name));
            }
        } catch (IOException e) {
            System.err.println("Error scanning data: " + e.getMessage());
            throw e;
        }
    }

    /*删除表
    * @param admin HBase管理员对象
    * @throws IOException 操作异常*/
    public static void deleteData(Admin admin) throws IOException {
        // 将表名转换为TableName对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 检查表是否存在
        if (admin.tableExists(tableName)) {
            // 如果存在，先禁用表，再删除表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table deleted successfully");// 输出删除成功的消息
        } else {
            // 如果表不存在，则输出提示信息
            System.out.println("Table does not exist");
        }
    }

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Admin admin = connection.getAdmin()) {
            //创建表
            createTable(admin);
            //插入数据
            insertData(connection);
            //获取单条数据
            getData(connection, "emp1");
            //扫描数据
            ScanData(connection);
            //删除表
            deleteData(admin);
        } catch (Exception e) {
            // 如果在上述操作中发生错误，输出错误信息
            System.err.println("An error occurred: " + e.getMessage());
            // 打印堆栈跟踪信息，有助于调试
            e.printStackTrace();
        }
    }
}