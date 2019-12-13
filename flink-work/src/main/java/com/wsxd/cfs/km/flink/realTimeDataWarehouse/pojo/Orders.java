package com.wsxd.cfs.km.flink.realTimeDataWarehouse.pojo;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/*
  *
  * @Date 2019/12/11 10:00
  * @methodName
  * @Param
  * @Return
  * 线上不适合用lombok这种注解的方式，它破坏了类的封装性
  **/
@Data
@ToString
public class Orders implements Serializable {
    private Integer orderId;

    private String orderNo;

    private Integer userId;

    private Integer goodId;

    private BigDecimal goodsMoney;

    private BigDecimal realTotalMoney;

    private Integer payFrom;

    private String province;

    private Timestamp createTime;

}
