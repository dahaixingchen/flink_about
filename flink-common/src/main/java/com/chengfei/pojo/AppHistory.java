package com.chengfei.pojo;

/**
 * @ClassName: AppHistory
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 14:23
 * @Version 1.0
 **/
public class AppHistory {
    private String id ;
    private String app_no;// '申请编号',
    private String app_no_cop;  //'合作机构申请编号',
    private String node_name;  //'流程环节名称',
    private String rtf_state;  //'审批状态',
    private String operator_id;  //'操作员ID',
    private String create_time;  //'创建时间',
    private String claim_time;  //'签收时间',
    private String remark;  //'审批原因备注',
    private String data_date;  //'数据日期',
    private String SYSTEM_SOURCE;  //'Z'
    public AppHistory(){}

    public AppHistory(String id, String app_no, String app_no_cop, String node_name, String rtf_state, String operator_id, String create_time, String claim_time, String remark, String data_date, String SYSTEM_SOURCE) {
        this.id = id;
        this.app_no = app_no;
        this.app_no_cop = app_no_cop;
        this.node_name = node_name;
        this.rtf_state = rtf_state;
        this.operator_id = operator_id;
        this.create_time = create_time;
        this.claim_time = claim_time;
        this.remark = remark;
        this.data_date = data_date;
        this.SYSTEM_SOURCE = SYSTEM_SOURCE;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getApp_no() {
        return app_no;
    }

    public void setApp_no(String app_no) {
        this.app_no = app_no;
    }

    public String getApp_no_cop() {
        return app_no_cop;
    }

    public void setApp_no_cop(String app_no_cop) {
        this.app_no_cop = app_no_cop;
    }

    public String getNode_name() {
        return node_name;
    }

    public void setNode_name(String node_name) {
        this.node_name = node_name;
    }

    public String getRtf_state() {
        return rtf_state;
    }

    public void setRtf_state(String rtf_state) {
        this.rtf_state = rtf_state;
    }

    public String getOperator_id() {
        return operator_id;
    }

    public void setOperator_id(String operator_id) {
        this.operator_id = operator_id;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getClaim_time() {
        return claim_time;
    }

    public void setClaim_time(String claim_time) {
        this.claim_time = claim_time;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getData_date() {
        return data_date;
    }

    public void setData_date(String data_date) {
        this.data_date = data_date;
    }

    public String getSYSTEM_SOURCE() {
        return SYSTEM_SOURCE;
    }

    public void setSYSTEM_SOURCE(String SYSTEM_SOURCE) {
        this.SYSTEM_SOURCE = SYSTEM_SOURCE;
    }

    @Override
    public String toString() {
        return "AppHistory{" +
                "id='" + id + '\'' +
                ", app_no='" + app_no + '\'' +
                ", app_no_cop='" + app_no_cop + '\'' +
                ", node_name='" + node_name + '\'' +
                ", rtf_state='" + rtf_state + '\'' +
                ", operator_id='" + operator_id + '\'' +
                ", create_time='" + create_time + '\'' +
                ", claim_time='" + claim_time + '\'' +
                ", remark='" + remark + '\'' +
                ", data_date='" + data_date + '\'' +
                ", SYSTEM_SOURCE='" + SYSTEM_SOURCE + '\'' +
                '}';
    }
}
