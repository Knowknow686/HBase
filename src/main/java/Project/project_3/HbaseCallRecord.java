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
//查询每个手机号码总时长
public class HbaseCallRecord {
    private static final String INPUT_TABLE = "call_records"; //map读取数据的表
    private static final String OUTPUT_TABLE = "outputtable";//reduce写入数据的表
    private static final String COLUMN_FAMILY = "call_info";
    private static final String QUALIFIER_CALL_DURATION = "duration";

    public static class TelecomMapper extends TableMapper<Text, IntWritable> {

        private final Text callerNumber = new Text();
        private final IntWritable callDuration = new IntWritable();

        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // 从 rowkey 中提取主叫号码
            String rowKey = Bytes.toString(row.get());
            String[] parts = rowKey.split("_");
            String caller = parts[0];
            System.out.println(caller);
            callerNumber.set(caller);
            // 获取通话时长
            byte[] durationBytes = value.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(QUALIFIER_CALL_DURATION));
            if (durationBytes != null) {
                int duration = Integer.parseInt(Bytes.toString(durationBytes));
                callDuration.set(duration);
                context.write(callerNumber, callDuration);
            }
        }
    }

    public static class TelecomReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalDuration = 0;
            //循环计算每个手机号码的通话总时长
            for (IntWritable val : values) {
                totalDuration += val.get();
            }
            //把处理完后的数据写入到hbase表中
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_call_duration"), Bytes.toBytes(totalDuration));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //指定zookeeper集群地址
//        conf.set("hbase.zookeeper.quorum","192.168.211.100,192.168.211.101,192.168.211.102");
        conf.set("hbase.zookeeper.quorum","hadoop100,hadoop101,hadoop102");
        conf.set("hadoop.native.lib", "false");


        // 创建输出表
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            TableName outputTableName = TableName.valueOf(OUTPUT_TABLE);
            if (!admin.tableExists(outputTableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(outputTableName);
                tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                admin.createTable(tableDescriptor);
                System.out.println("Output table created: " + OUTPUT_TABLE);
            }
        }

        Job job = Job.getInstance(conf, "HbaseCallRecord");
        job.setJarByClass(HbaseCallRecord.class);

        Scan scan = new Scan();

        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(QUALIFIER_CALL_DURATION));
        //指定map的输入数据
        TableMapReduceUtil.initTableMapperJob(
                INPUT_TABLE,
                scan,
                TelecomMapper.class,
                Text.class,
                IntWritable.class,
                job
        );
        //指定reduce的输出
        TableMapReduceUtil.initTableReducerJob(
                OUTPUT_TABLE,
                TelecomReducer.class,
                job
        );
        // 提交Job
        boolean success = job.waitForCompletion(true);
        if(success){
            System.out.println("成功");
        }else {
            System.out.println("失败");
        }
    }
}