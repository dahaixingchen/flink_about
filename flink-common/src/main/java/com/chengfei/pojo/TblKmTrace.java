package com.chengfei.pojo;

import java.util.Date;

/**
 * @ClassName: TblKmTrace
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/30 18:12
 * @Version 1.0
 **/
public class TblKmTrace {

    private String sys_date;
    private String merchantno;
    private String saledate;
    private String shop;
    private String id;
    private String name;
    private String qty;
    private String amount;
    private String refundqty;
    private String refundamt;
    private String create_time;
    private String update_time;

    public TblKmTrace() {
    }

    @Override
    public String toString() {
        return "TblKmTrace{" +
                "sys_date='" + sys_date + '\'' +
                ", merchantno='" + merchantno + '\'' +
                ", saledate='" + saledate + '\'' +
                ", shop='" + shop + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", qty='" + qty + '\'' +
                ", amount='" + amount + '\'' +
                ", refundqty='" + refundqty + '\'' +
                ", refundamt='" + refundamt + '\'' +
                ", create_time='" + create_time + '\'' +
                ", update_time='" + update_time + '\'' +
                '}';
    }

    public String getSys_date() {
        return sys_date;
    }

    public void setSys_date(String sys_date) {
        this.sys_date = sys_date;
    }

    public String getMerchantno() {
        return merchantno;
    }

    public void setMerchantno(String merchantno) {
        this.merchantno = merchantno;
    }

    public String getSaledate() {
        return saledate;
    }

    public void setSaledate(String saledate) {
        this.saledate = saledate;
    }

    public String getShop() {
        return shop;
    }

    public void setShop(String shop) {
        this.shop = shop;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQty() {
        return qty;
    }

    public void setQty(String qty) {
        this.qty = qty;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getRefundqty() {
        return refundqty;
    }

    public void setRefundqty(String refundqty) {
        this.refundqty = refundqty;
    }

    public String getRefundamt() {
        return refundamt;
    }

    public void setRefundamt(String refundamt) {
        this.refundamt = refundamt;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }
}
