package Project.project_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
//统计每天每个主叫号码的通话次数
public class HBaseMapReduceDailyCallerCount {

    private static final String Input_Table = "call_records";
    private static final String Output_Table = "telecom_daily_stats";
    private static final String Column_Family = "call_info";
    private static final String Qualifier_Call_Time = "call_time";

    public static class DailyCallerMapper extends TableMapper<Text, IntWritable> {

        private final Text dailyCallerKey = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // 从 rowkey 中提取主叫号码
            String rowKey = Bytes.toString(row.get());
            String[] parts = rowKey.split("_");
            String caller = parts[0];

            // 获取通话时间
            byte[] callTimeBytes = value.getValue(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Time));
            if (callTimeBytes != null) {
                String callTime = Bytes.toString(callTimeBytes);
                String date = callTime.split(" ")[0];

                // 组合成日期_主叫号码的键
                String key = date + "_" + caller;
                dailyCallerKey.set(key);
                context.write(dailyCallerKey, one);
            }
        }
    }

    public static class DailyCallerReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            //把统计后的数据写入到hbase表中
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes("call_count"), Bytes.toBytes(count));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //指定zookeeper集群地址
        conf.set("hbase.zookeeper.quorum","192.168.211.100,192.168.211.101,192.168.211.102");
        // 创建输出表
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            TableName outputTableName = TableName.valueOf(Output_Table);
            if (!admin.tableExists(outputTableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(outputTableName);
                tableDescriptor.addFamily(new HColumnDescriptor(Column_Family));
                admin.createTable(tableDescriptor);
                System.out.println("Output table created: " + Output_Table);
            }
        }

        Job job = Job.getInstance(conf, "HBaseMapReduceDailyCallerCount");
        job.setJarByClass(HBaseMapReduceDailyCallerCount.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Time));

        TableMapReduceUtil.initTableMapperJob(
                Input_Table,
                scan,
                DailyCallerMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                Output_Table,
                DailyCallerReducer.class,
                job
        );

        boolean success = job.waitForCompletion(true);
        if(success){
            System.out.println("成功");
        }else {
            System.out.println("失败");
        }
    }
}