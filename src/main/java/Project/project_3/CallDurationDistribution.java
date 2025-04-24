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
//1. 统计每个主叫号码的通话时长分布
// 可以将通话时长划分为不同的区间（如 0 - 1 分钟、1 - 5 分钟、5 - 10 分钟等），统计每个主叫号码在各个区间的通话次数。
public class CallDurationDistribution {

    private static final String Input_Table = "call_records";
    private static final String Output_Table = "call_duration_distribution";
    private static final String Column_Family = "call_info";
    private static final String Qualifier_Call_Duration = "duration";
    private static final String Qualifier_User_ID = "user_id";

    public static class DurationDistributionMapper extends TableMapper<Text, IntWritable> {

        private final Text callerDurationKey = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(row.get());
            String[] parts = rowKey.split("_");
            String caller = parts[0];

            byte[] durationBytes = value.getValue(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Duration));
            if (durationBytes != null) {
                int duration = Integer.parseInt(Bytes.toString(durationBytes));
                String durationRange = getDurationRange(duration);
                String key = caller + "_" + durationRange;
                callerDurationKey.set(key);
                context.write(callerDurationKey, one);
            }
        }

        private String getDurationRange(int duration) {
            if (duration < 60) {
                return "0 - 1 分钟";
            } else if (duration < 300) {
                return "1 - 5 分钟";
            } else if (duration < 600) {
                return "5 - 10 分钟";
            } else {
                return "10 分钟以上";
            }
        }
    }

    public static class DurationDistributionReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes("call_count"), Bytes.toBytes(count));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //指定zookeeper集群地址
        conf.set("hbase.zookeeper.quorum","192.168.211.100,192.168.211.101,192.168.211.102");

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

        Job job = Job.getInstance(conf, "CallDurationDistribution");
        job.setJarByClass(CallDurationDistribution.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Duration));

        TableMapReduceUtil.initTableMapperJob(
                Input_Table,
                scan,
                DurationDistributionMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                Output_Table,
                DurationDistributionReducer.class,
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
