package cn.ktte.log;

public class MyClickLog {

    //定义日志相关的字段
    private String userClientIp;

    public String getUserClientIp() {
        return userClientIp;
    }

    public void setUserClientIp(String userClientIp) {
        this.userClientIp = userClientIp;
    }

    @Override
    public String toString() {
        return "MyClickLog{" +
                "userClientIp='" + userClientIp + '\'' +
                '}';
    }
}
