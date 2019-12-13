package com.wsxd.cfs.km.flink.realTimeDataWarehouse.simulator;

import com.wsxd.cfs.km.flink.realTimeDataWarehouse.pojo.Orders;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import sun.rmi.runtime.Log;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * @ClassName: OrderSource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/11 10:54
 * @Version 1.0
 **/
public class OrderSource extends RichParallelSourceFunction<Row> {

    private static final Orders orders = new Orders();
    private static final Random random = new Random();
    private volatile boolean isRunning = true;
    public static final Map<String,String> PROVINCE_MAP= new ImmutableMap
            .Builder<String, String>()
            .put("1","北京")
            .put("2","上海")
            .put("3","天津")
            .put("4","重庆")
            .put("5","黑龙江")
            .put("6","吉林")
            .put("7","辽宁")
            .put("8","内蒙古")
            .put("9","河北")
            .put("10","新疆")
            .put("11","甘肃")
            .put("12","青海")
            .put("13","陕西")
            .put("14","宁夏")
            .put("15","河南")
            .put("16","山东")
            .put("17","山西")
            .put("18","安徽")
            .put("19","湖北")
            .put("20","湖南")
            .put("21","江苏")
            .put("22","四川")
            .put("23","贵州")
            .put("24","云南")
            .put("25","广西")
            .put("26","西藏")
            .put("27","浙江")
            .put("28","江西")
            .put("29","广东")
            .put("30","福建")
            .put("31","台湾")
            .put("32","海南")
            .put("33","香港")
            .put("34","澳门")
            .build();

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        while (isRunning){
            orders.setOrderNo(UUID.randomUUID().toString().substring(0,12));
            //用户编号从10000到100000
            orders.setUserId(random.nextInt(100000)%(100000-10000+1)+10000);
            //产品编号从1到6
            orders.setGoodId(random.nextInt(6)+1);
            //产品价格从0到1000保留两位有小数
            orders.setGoodsMoney(BigDecimal.valueOf(Math.round(random.nextDouble()*1000)/1000.0));
            //产品真正的总价
            orders.setRealTotalMoney(BigDecimal.valueOf(Math.round(random.nextDouble()*1000)/1000.0));
            //付款渠道
            orders.setPayFrom(random.nextInt(2)+1);
            //省份
            orders.setProvince(String.valueOf(random.nextInt(34)+1));

            ctx.collect(Row.of(
                    orders.getOrderNo(),
                    orders.getUserId(),
                    orders.getGoodId(),
                    orders.getGoodsMoney(),
                    orders.getRealTotalMoney(),
                    orders.getPayFrom(),
                    PROVINCE_MAP.get(orders.getProvince())
            ));
            Thread.sleep((long) (Math.random()+1)*2000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
