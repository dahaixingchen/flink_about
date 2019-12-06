import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName: ThreadPoolTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/5 16:01
 * @Version 1.0
 **/
public class ThreadPoolTest {
    //线程池
    public static void exec(List<String> list) throws InterruptedException {
        int count = 3;                   //一个线程处理300条数据
        int listSize = list.size();        //数据集合大小
        int runSize = (listSize / count) + 1;  //开启的线程数
        List<String> newlist = null;       //存放每个线程的执行数据
        ExecutorService executor = Executors.newFixedThreadPool(runSize);      //创建一个线程池，数量和开启线程的数量一样
//创建两个个计数器
        CountDownLatch begin = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(runSize);
        //循环创建线程
        for (int i = 0; i < runSize; i++) {
            //计算每个线程执行的数据
            if ((i + 1) == runSize) {
                int startIndex = (i * count);
                int endIndex = list.size();
                newlist = list.subList(startIndex, endIndex);
            } else {
                int startIndex = (i * count);
                int endIndex = (i + 1) * count;
                newlist = list.subList(startIndex, endIndex);
            }
            //线程类
            MyThread mythead = new MyThread(newlist, begin, end);
            //这里执行线程的方式是调用线程池里的executor.execute(mythead)方法。

            executor.execute(mythead);
        }

        begin.countDown();
        end.await();

        //执行完关闭线程池
        executor.shutdown();
    }


    //测试
    public static void main(String[] args) {
        List<String> list = new ArrayList<String>();
        //数据越大线程越多
        for (int i = 0; i < 30; i++) {
            list.add("hello" + i);
        }

        try {
            exec(list);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class MyThread implements Runnable {
        private List<String> list;
        private CountDownLatch begin;
        private CountDownLatch end;

        //创建个构造函数初始化 list,和其他用到的参数
        public MyThread(List<String> list, CountDownLatch begin, CountDownLatch end) {
            this.list = list;
            this.begin = begin;
            this.end = end;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < list.size(); i++) {
                    //这里还要说一下，，由于在实质项目中，当处理的数据存在等待超时和出错会使线程一直处于等待状态
                    //这里只是处理简单的，
                    //分批 批量插入
                }

                //执行完让线程直接进入等待
                begin.await();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                //这里要主要了，当一个线程执行完 了计数要减一不然这个线程会被一直挂起
// ，end.countDown()，这个方法就是直接把计数器减一的
                end.countDown();
            }
        }

    }

    public static ExecutorService executorService = Executors.newCachedThreadPool();
    private final static int num = 20;
    private static CountDownLatch cdl = new CountDownLatch(30);
    private static final Random random = new Random();

    public void test() {
        for (int i = 0; i < num; i++)
            executorService.execute(new ThreadTest());
    }

    @Test
    public void testThreadPool(){
        test();

        //插入数据完成后  执行修改操作
        try {
            cdl.await();
        } catch (InterruptedException e) {
        }
        System.out.println("它们已经插完啦..............................");
        executorService.shutdown();

    }

    class ThreadTest implements Runnable {

        public void run() {
            //执行插入数据操作  每次插入一条
            // 模拟耗时
            int time = random.nextInt(10000);
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
            }
            System.out.println(Thread.currentThread().getName() + "执行完了，耗时：" + time / 1000 + "秒");
            cdl.countDown();
        }
    }
}
