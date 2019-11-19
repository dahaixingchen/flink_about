package com.chengfei.pojo;

/**
 * @ClassName: AppHistory
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/19 10:10
 * @Version 1.0
 **/
public class AppHistoryAbout {
    private String key;
    private String appHistorydata;
    private String total;

    public AppHistoryAbout() {
    }

    public AppHistoryAbout(String key, String appHistorydata, String total) {
        this.key = key;
        this.appHistorydata = appHistorydata;
        this.total = total;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getAppHistorydata() {
        return appHistorydata;
    }

    public void setAppHistorydata(String appHistorydata) {
        this.appHistorydata = appHistorydata;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }
}
