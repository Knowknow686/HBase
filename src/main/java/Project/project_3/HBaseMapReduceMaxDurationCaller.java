package Project.project_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

//找出通话时长最长的主叫号码
public class HBaseMapReduceMaxDurationCaller {
    private static final String Input_Table = "call_records";
    private static final String Output_Table = "telecom_max_duration_stats";
    private static final String Column_Family = "call_info";
    private static final String Qualifier_Call_Duration = "duration";

    public static class MaxDurationMapper extends TableMapper<Text, IntWritable> {

        private final Text callerNumber = new Text();
        private final IntWritable callDuration = new IntWritable();

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String rowKey = Bytes.toString(row.get());
            String[] parts = rowKey.split("_");
            String caller = parts[0];
            callerNumber.set(caller);

            byte[] durationBytes = value.getValue(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Duration));
            if (durationBytes != null) {
                int duration = Integer.parseInt(Bytes.toString(durationBytes));
                callDuration.set(duration);
                context.write(callerNumber, callDuration);
            }
        }
    }

    public static class MaxDurationReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        private String maxCaller = "";
        private int maxDuration = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalDuration = 0;
            for (IntWritable val : values) {
                totalDuration += val.get();
            }

            if (totalDuration > maxDuration) {
                maxDuration = totalDuration;
                maxCaller = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(maxCaller));
            put.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes("max_call_duration"), Bytes.toBytes(maxDuration));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(maxCaller)), put);
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

        Job job = Job.getInstance(conf, "HBaseMapReduceMaxDurationCaller");
        job.setJarByClass(HBaseMapReduceMaxDurationCaller.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(Column_Family), Bytes.toBytes(Qualifier_Call_Duration));

        TableMapReduceUtil.initTableMapperJob(
                Input_Table,
                scan,
                MaxDurationMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                Output_Table,
                MaxDurationReducer.class,
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