package com.wsxd.cfs.km.flink.pojo;

/**
 * @ClassName: Trace
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 10:25
 * @Version 1.0
 **/
public class Trace {
    private String merchantno;
    private String saledate;
    private String shop;
    private String id;
    private String name;
    private String qty;
    private String amount;
    private String refundqty;
    private String refundamt;

    public Trace() {
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

    @Override
    public String toString() {
        return "Trace{" +
                "merchantno='" + merchantno + '\'' +
                ", saledate='" + saledate + '\'' +
                ", shop='" + shop + '\'' +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", qty='" + qty + '\'' +
                ", amount='" + amount + '\'' +
                ", refundqty='" + refundqty + '\'' +
                ", refundamt='" + refundamt + '\'' +
                '}';
    }
}
