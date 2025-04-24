package Project.project_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
//可以按月份统计每个小时的通话次数，找出每个月通话次数最多的小时作为通话高峰时段
public class MonthlyPeakHours {

    private static final String  Input_Table = "call_records";//读取的hbase表名
    private static final String  Output_Table = "monthly_peak_hours";//把处理的结果保存在hbase表中
    private static final String  Column_Family = "call_info";//列族
    private static final String  Qualifier_Call_Time = "call_time";//列族中的通过时长列

    public static class MonthlyPeakMapper extends TableMapper<Text, IntWritable> {

        private final Text monthHourKey = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            byte[] callTimeBytes = value.getValue(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Time));
            if (callTimeBytes != null) {
                String callTime = Bytes.toString(callTimeBytes);
                String month = callTime.split("-")[1];
                String hour = callTime.split(" ")[1].split(":")[0];
                String key = month + "_" + hour;
                monthHourKey.set(key);
                context.write(monthHourKey, one);
            }
        }
    }

    public static class MonthlyPeakReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            String[] parts = key.toString().split("_");
            String month = parts[0];
            String hour = parts[1];

            Put put = new Put(Bytes.toBytes(month));
            put.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes(hour), Bytes.toBytes(count));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(month)), put);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //指定zookeeper集群地址
        conf.set("hbase.zookeeper.quorum", "192.168.211.100,192.168.211.101,192.168.211.102");
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

        Job job = Job.getInstance(conf, "MonthlyPeakHours");
        job.setJarByClass(MonthlyPeakHours.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Time));
        TableMapReduceUtil.initTableMapperJob(
                Input_Table,
                scan,
                MonthlyPeakMapper.class,
                Text.class,
                IntWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                Output_Table,
                MonthlyPeakReducer.class,
                job);

        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("HbaseMapReduceIntegration Job successful");
        } else {
            System.out.println("HbaseMapReduceIntegration Job failed");
        }
    }
}
