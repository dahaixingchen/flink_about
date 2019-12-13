import org.testng.annotations.Test;

import java.util.Random;
import java.util.UUID;

/**
 * @ClassName: RundomTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/11 11:00
 * @Version 1.0
 **/
public class RundomTest {
    /**
      *
      * @Date 2019/12/11 11:07
      * @methodName rundomTest
      * @Param []
      * @Return void
     *
     * orderNo=$Func{uuid()}
     * userId=$Func{intRand(10000,99999)}
     * goodId=$Func{intRand(1,5)}
     * goodsMoney=$Func{doubleRand(2099, 100000, 2)}
     * realTotalMoney=$Func{doubleRand(1000, 100000, 2)}
     * payFrom=1|||2|||1|||1|||2
     * province=$Func{intRand(1,34)}
      **/
    @Test
    public void rundomTest(){
        Random random = new Random();
        int max = 100;
        int min = 98;
        //orderNo
        System.out.println(UUID.randomUUID().toString().substring(0,12));
        int i = random.nextInt(max);
//        System.out.println(random.nextInt(max)%(max-min+1) + min);

//        System.out.println(String.valueOf(random.nextDouble()*1000));

//        System.out.println(random.nextGaussian());
//        System.out.println(random.nextInt(1)+1);
        System.out.println(Math.random());
//        System.out.println(Math.round(144.6235*1000)/1000.0);

    }
}
