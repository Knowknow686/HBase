package Project.project_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

// 管理通话记录的类
public class CallRecordManager {
    // 定义HBase表名
    private static final String Table_Name = "Call_Records";
    // 定义HBase列族名，用于存储通话信息
    private static final String Column_Family_Call_Info = "Call_Info";
    // 定义HBase列族名，用于存储客户信息
    private static final String Column_Family_Customer_Info = "Customer_Info";

    // HBase连接对象
    private Connection connection;
    // HBase管理对象
    private Admin admin;

    // 构造函数，初始化HBase连接和管理对象
    public CallRecordManager() {
        try {
            // 创建HBase配置对象
            Configuration config = HBaseConfiguration.create();
            // 指定zookeeper集群地址，用于HBase的分布式协调
            config.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
            // 创建HBase连接
            connection = ConnectionFactory.createConnection(config);
            // 获取HBase管理对象
            admin = connection.getAdmin();
        } catch (Exception e) {
            // 打印异常堆栈信息
            e.printStackTrace();
        }
    }

    // 创建通话记录表
    public void createTable() throws Exception {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 检查表是否存在
        if (!admin.tableExists(tableName)) {
            // 创建表描述符对象
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            // 添加列族描述符，用于存储通话信息
            tableDescriptor.addFamily(new HColumnDescriptor(Column_Family_Call_Info));
            // 添加列族描述符，用于存储客户信息
            tableDescriptor.addFamily(new HColumnDescriptor(Column_Family_Customer_Info));
            // 创建表
            admin.createTable(tableDescriptor);
            // 输出表创建成功的消息
            System.out.println("Table created successfully");
        } else {
            // 输出表已经存在的消息
            System.out.println("Table already exists");
        }
    }

    // 删除通话记录表
    public void deleteTable() throws IOException {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 检查表是否存在
        if (admin.tableExists(tableName)) {
            // 禁用表
            admin.disableTable(tableName);
            // 删除表
            admin.deleteTable(tableName);
            // 输出表删除成功的消息
            System.out.println("Table deleted successfully");
        } else {
            // 输出表不存在的消息
            System.out.println("Table does not exist");
        }
    }

    // 插入通话记录
    public void insertCallRecord(String caller, String callee, String callTime, String duration) throws Exception {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 构造rowKey，格式为：caller_callTime(无非数字字符)
        //使用callTime.replaceAll("[^0-9]", "")；是为了把通话时间字符串转换为纯数字字符串。
        //进而将其作为行键的一部分，以便于在查询时进行精确匹配。
        String rowKey = caller + "_" + callTime;//callTime.replaceAll("[^0-9]", "")
        // 创建Put对象，用于插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        // 添加列族和列的值，callee号码
        put.addColumn(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("callee"), Bytes.toBytes(callee));
        // 添加列族和列的值，callTime时间
        put.addColumn(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("call_time"), Bytes.toBytes(callTime));
        // 添加列族和列的值，duration通话时长
        put.addColumn(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        // 添加列族和列的值，caller号码
        put.addColumn(Bytes.toBytes(Column_Family_Customer_Info), Bytes.toBytes("caller"), Bytes.toBytes(caller));
        // 插入数据到表中
        table.put(put);
        table.close();
        // 输出通话记录插入成功的消息
        System.out.println("Call record inserted successfully");
    }

    // 查询主叫通话记录
    public List<CallRecord> queryCallRecords(String caller, String startTime, String endTime) throws IOException, ParseException {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 创建Scan对象，用于扫描表中的数据
        Scan scan = new Scan();
        // 创建FilterList对象，用于组合多个过滤器
        /*FilterList.Operator.MUST_PASS_ALL表示在一个FilterList中，所有的过滤器都必须通过过滤条件，
        * 才能让一条数据记录被视为匹配。也就是说，只有当某条记录同时及满足FilterList中所有的过滤条件时，
        * 这条记录才会被返回。否则，该条记录将被过滤掉*/
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 如果主叫号码不为空，则添加前缀过滤器
        if (caller != null && !caller.isEmpty()) {
            filterList.addFilter(new PrefixFilter(Bytes.toBytes(caller + "_")));
        }

        // 如果开始时间不为空，则添加单列值过滤器，筛选开始时间之后的数据
        if (startTime != null && !startTime.isEmpty()) {
            //long startTimeStamp = getTimeStamp(startTime);
            SingleColumnValueFilter startFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(Column_Family_Call_Info),
                    Bytes.toBytes("call_time"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,//大于等于
                    new SubstringComparator("_"+startTime));
            filterList.addFilter(startFilter);
        }

        // 如果结束时间不为空，则添加单列值过滤器，筛选结束时间之前的数据
        if (endTime != null && !endTime.isEmpty()) {
            //long endTimeStamp = getTimeStamp(endTime);
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(Column_Family_Call_Info),
                    Bytes.toBytes("call_time"),
                    CompareFilter.CompareOp.LESS_OR_EQUAL,//小于等于
                    new SubstringComparator("_"+endTime));
            filterList.addFilter(endFilter);
        }

        // 设置扫描的过滤器列表
        scan.setFilter(filterList);
        // 获取扫描结果迭代器
        ResultScanner scanner = table.getScanner(scan);
        // 创建一个List来存储查询到的通话记录
        List<CallRecord> callRecords = new ArrayList<>();
        // 遍历扫描结果
        for (Result result : scanner) {
            // 获取rowKey
            byte[] row = result.getRow();
            // 获取callee号码
            byte[] callee = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("callee"));
            // 获取callTime时间
            byte[] callTime = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("call_time"));
            // 获取duration通话时长
            byte[] duration = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("duration"));
            // 获取caller号码
            byte[] callerBytes = result.getValue(Bytes.toBytes(Column_Family_Customer_Info), Bytes.toBytes("caller"));
            // 构造一个CallRecord对象
            CallRecord callRecord = new CallRecord(
                    Bytes.toString(row),
                    Bytes.toString(callerBytes),
                    Bytes.toString(callee),
                    Bytes.toString(callTime),
                    Bytes.toString(duration));
            // 通话记录添加到集合中
            callRecords.add(callRecord);
        }
        // 关闭扫描结果迭代器
        scanner.close();
        // 关闭表对象
        table.close();
        // 返回通话记录列表
        return callRecords;
    }

    // 查询被叫通话记录
    public List<CallRecord> queryRecords(String callee, int time) throws IOException, ParseException {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 创建Scan对象，用于扫描表中的数据
        Scan scan = new Scan();
        // 创建FilterList对象，用于组合多个过滤器
        /*FilterList.Operator.MUST_PASS_ALL表示在一个FilterList中，所有的过滤器都必须通过过滤条件，
         * 才能让一条数据记录被视为匹配。也就是说，只有当某条记录同时及满足FilterList中所有的过滤条件时，
         * 这条记录才会被返回。否则，该条记录将被过滤掉*/
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 被叫号码过滤器
        SingleColumnValueFilter calleeFilter = new SingleColumnValueFilter(
                Bytes.toBytes("call_info"),
                Bytes.toBytes("callee"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(callee));
        filterList.addFilter(calleeFilter);

        // 通话时长过滤器
        SingleColumnValueFilter durationFilter = new SingleColumnValueFilter(
                Bytes.toBytes("call_info"),
                Bytes.toBytes("duration"),
                CompareFilter.CompareOp.GREATER,
                Bytes.toBytes(time));
        filterList.addFilter(durationFilter);

        // 设置扫描的过滤器列表
        scan.setFilter(filterList);
        // 获取扫描结果迭代器
        ResultScanner scanner = table.getScanner(scan);
        // 创建一个List来存储查询到的通话记录
        List<CallRecord> callRecords = new ArrayList<>();
        // 遍历扫描结果
        for (Result result : scanner) {
            // 获取rowKey
            byte[] row = result.getRow();
            // 获取callee号码
            byte[] calleeNumber = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("callee"));
            // 获取callTime时间
            byte[] callTime = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("call_time"));
            // 获取duration通话时长
            byte[] duration = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("duration"));
            // 获取caller号码
            byte[] callerBytes = result.getValue(Bytes.toBytes(Column_Family_Customer_Info), Bytes.toBytes("caller"));
            // 构造一个CallRecord对象
            CallRecord record = new CallRecord(
                    Bytes.toString(row),
                    Bytes.toString(callerBytes),
                    Bytes.toString(calleeNumber),
                    Bytes.toString(callTime),
                    Bytes.toString(duration));
            // 通话记录添加到集合中
            callRecords.add(record);
        }
        // 关闭扫描结果迭代器
        scanner.close();
        // 关闭表对象
        table.close();
        // 返回通话记录列表
        return callRecords;
    }

    // 删除通话记录
    public void deleteCallRecord(String rowKey) throws Exception {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 创建Delete对象，用于删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除数据
        table.delete(delete);
        // 关闭表对象
        table.close();
        // 输出通话记录删除成功的消息
        System.out.println("Call record deleted successfully");
    }

    // 统计用户通话次数
    public int countCallsByCaller(String caller) throws IOException {
        // 获取表名对象
        TableName tableName = TableName.valueOf(Table_Name);
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 创建Scan对象，用于扫描表中的数据
        Scan scan = new Scan();
        // 设置行前缀过滤器，筛选指定主叫号码的数据
        scan.setRowPrefixFilter((caller + "_").getBytes());
        // 获取扫描结果迭代器
        ResultScanner scanner = table.getScanner(scan);
        // 初始化计数器
        int count = 0;
        // 遍历扫描结果
        for (Result result : scanner) {
            count++;
        }
        // 输出统计结果
        System.out.println("Caller: " + caller + " made " + count + " calls.");
        return count;
    }

    // 高频通话对查询实现思路：将主叫号码和被叫号码组合一个唯一的通话对，统计每个通话对的通话次数，找出通话次数最多的通话对。
    public void FrequencyCallPairQuery() {
        TableName tableName = TableName.valueOf(Table_Name);
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = connection.getTable(tableName);
            Scan scan = new Scan();
            scanner = table.getScanner(scan);
            // 统计每个通话对的通话次数
            Map<String, Integer> callPairCount = new HashMap<>();
            for (Result result : scanner) {
                byte[] callerBytes = result.getValue(Bytes.toBytes(Column_Family_Customer_Info), Bytes.toBytes("caller"));
                byte[] calleeBytes = result.getValue(Bytes.toBytes(Column_Family_Call_Info), Bytes.toBytes("callee"));
                if (callerBytes != null && calleeBytes != null) {
                    String caller = Bytes.toString(callerBytes);
                    String callee = Bytes.toString(calleeBytes);
                    String callPair = caller + "_" + callee;
                    callPairCount.put(callPair, callPairCount.getOrDefault(callPair, 0) + 1);
                }
            }
            // 找出高频通话对
            String frequentCallPair = "";
            int maxCallCount = 0;
            for (Map.Entry<String, Integer> entry : callPairCount.entrySet()) {
                if (entry.getValue() > maxCallCount) {
                    maxCallCount = entry.getValue();
                    frequentCallPair = entry.getKey();
                }
            }
            System.out.println("高频通话对为：" + frequentCallPair + "，通话次数为：" + maxCallCount);
        } catch (IOException e) {
            System.err.println("查询高频通话对时发生IO异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 确保资源被正确关闭
            try {
                if (scanner != null) {
                    scanner.close();
                }
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                System.err.println("关闭资源时发生IO异常: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    // 关闭HBase连接和管理对象
    public void close() {
        try {
            // 关闭HBase管理对象
            if (admin != null) {
                admin.close();
            }
            // 关闭HBase连接
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            // 打印异常堆栈信息
            e.printStackTrace();
        }
    }

    // 将日期字符串转换为时间戳
    private long getTimeStamp(String dateStr) throws Exception {
        // 创建日期格式化对象，指定日期格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 将日期字符串解析为Date对象
        Date date = sdf.parse(dateStr);
        // 返回Date对象对应的时间戳
        return date.getTime();
    }
}