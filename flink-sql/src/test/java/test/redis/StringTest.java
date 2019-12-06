package test.redis;

import java.text.DecimalFormat;

/**
 * @ClassName: StringTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/2 14:12
 * @Version 1.0
 **/
public class StringTest {
    public static void main(String[] args) {
        double x1=0.006126;
        System.out.println(String.format("%.2f",x1));
//        DecimalFormat df = new DecimalFormat(".00");
//        System.out.println(df.format(x1));

    }
}
