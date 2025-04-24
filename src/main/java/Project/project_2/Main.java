package Project.project_2;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.text.SimpleDateFormat;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        CallRecordManager manager = new CallRecordManager();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Random random = new Random();
        String[] phoneNumbers = {"13800138000", "13900139000", "13700137000", "13600136000"};

        try {
            // 创建表
            manager.createTable();

            // 插入记录
            for (int i = 0; i < 10; i++) {
                String caller = getRandomPhoneNumber(phoneNumbers, random);
                String receiver;
                do {
                    receiver = getRandomPhoneNumber(phoneNumbers, random);
                } while (caller.equals(receiver)); // 确保主叫和被叫号码不同
                Date callDateTime = getRandomCallDateTime(random);
                String callTime = dateFormat.format(callDateTime);
                int durationInSeconds = 30 + random.nextInt(300); // 随机生成30到329秒的通话时长
                String duration = String.valueOf(durationInSeconds); // 将通话时长转换为字符串
                manager.insertCallRecord(caller, receiver, callTime, duration);
            }


            // 查询记录并打印
            printCallRecords(manager.queryCallRecords(null, null, null));

//            // 统计主叫用户通话次数
//            String callerNumber = "13600136000";
//            System.out.println("主叫号码 " + callerNumber + " 的通话次数: " + manager.countCallsByCaller(callerNumber));
//
//            // 查询高频通话对
//            manager.FrequencyCallPairQuery();
//
//            // 查询被叫通话记录并打印
//            String calleeNumber = "13800138000";
//            int durationThreshold = 30;
//            printCallRecords(manager.queryRecords(calleeNumber, durationThreshold));
//
//            // 删除记录
//            manager.deleteCallRecord("13600136000_2025-02-12 15:44:05");
//            // 再次查询记录并打印
//            printCallRecords(manager.queryCallRecords(null, null, null));
//
//            // 删除表
//            manager.deleteTable();
//
//            // 关闭连接
//            manager.close();

        } catch (SQLException e) {
            System.err.println("数据库操作发生异常: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("其他异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 获取随机的手机号码
    private static String getRandomPhoneNumber(String[] phoneNumbers, Random random) {
        return phoneNumbers[random.nextInt(phoneNumbers.length)];
    }

    // 获取随机的通话日期时间
    private static Date getRandomCallDateTime(Random random) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2025, Calendar.FEBRUARY, 1 + random.nextInt(28), random.nextInt(24), random.nextInt(60), random.nextInt(60));
        return calendar.getTime();
    }

    // 打印通话记录
    private static void printCallRecords(List<CallRecord> records) {
        if (records == null || records.isEmpty()) {
            System.out.println("没有找到通话记录。");
        } else {
            for (CallRecord record : records) {
                System.out.println("主叫号码: " + record.getCaller() +
                                   ", 被叫号码: " + record.getCallee() +
                                   ", 通话时间: " + record.getStartTime() +
                                   ", 通话时长: " + record.getDuration());
            }
        }
    }
}