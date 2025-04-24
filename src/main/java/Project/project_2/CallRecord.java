package Project.project_2;

/**
 * 通话记录类，用于存储和操作单个通话记录的信息
 */
public class CallRecord {
    // 唯一标识每一条通话记录的键
    private String rowKey;
    // 通话发起者的电话号码
    private String caller;
    // 通话接收者的电话号码
    private String callee;
    // 通话开始的时间，格式可以根据实际需求调整，例如 "yyyy-MM-dd HH:mm:ss"
    private String startTime;
    // 通话的持续时间，单位为秒
    private String duration;

    /**
     * 构造函数，用于创建一个新的通话记录对象
     * @param rowKey    唯一标识每一条通话记录的键
     * @param caller    通话发起者的电话号码
     * @param callee    通话接收者的电话号码
     * @param startTime 通话开始的时间
     * @param duration  通话的持续时间（秒）
     */
    public CallRecord(String rowKey, String caller, String callee, String startTime, String duration) {
        this.rowKey = rowKey;
        this.caller = caller;
        this.callee = callee;
        this.startTime = startTime;
        this.duration = duration;
    }

    /**
     * 空构造函数，用于创建没有任何属性的通话记录对象
     */
    public CallRecord() {
    }

    /**
     * 获取通话记录的唯一标识键
     * @return 唯一标识键
     */
    public String getRowKey() {
        return rowKey;
    }

    /**
     * 设置通话记录的唯一标识键
     * @param rowKey 唯一标识键
     */
    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * 获取通话发起者的电话号码
     * @return 通话发起者的电话号码
     */
    public String getCaller() {
        return caller;
    }

    /**
     * 设置通话发起者的电话号码
     * @param caller 通话发起者的电话号码
     */
    public void setCaller(String caller) {
        this.caller = caller;
    }

    /**
     * 获取通话接收者的电话号码
     * @return 通话接收者的电话号码
     */
    public String getCallee() {
        return callee;
    }

    /**
     * 设置通话接收者的电话号码
     * @param callee 通话接收者的电话号码
     */
    public void setCallee(String callee) {
        this.callee = callee;
    }

    /**
     * 获取通话开始的时间
     * @return 通话开始的时间
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * 设置通话开始的时间
     * @param startTime 通话开始的时间
     */
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    /**
     * 获取通话的持续时间
     * @return 通话的持续时间（秒）
     */
    public String getDuration() {
        return duration;
    }

    /**
     * 设置通话的持续时间
     * @param duration 通话的持续时间（秒）
     */
    public void setDuration(String duration) {
        this.duration = duration;
    }

    /**
     * 返回通话记录的字符串表示
     * @return 包含通话记录所有信息的字符串
     */
    @Override
    public String toString() {
        return "CallRecord{" +
                "rowKey='" + rowKey + '\'' +
                ", caller='" + caller + '\'' +
                ", callee='" + callee + '\'' +
                ", startTime='" + startTime + '\'' +
                ", duration=" + duration +
                '}';
    }
}