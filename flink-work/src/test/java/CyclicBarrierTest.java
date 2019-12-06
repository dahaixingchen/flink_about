import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @ClassName: CyclicBarrierTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/5 14:06
 * @Version 1.0
 **/
public class CyclicBarrierTest {
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        int threadNmu = 5;
        CyclicBarrier barrier = new CyclicBarrier(threadNmu, new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName()+ " 完成最后任务");
            }
        });
//        barrier.await();
        for (int i = 0;i<threadNmu;i++){
            Thread thread = new Thread(new TaskThread(barrier));
            thread.start();
            System.out.println("我是主线程");
        }
        barrier.await();
    }

    private static class TaskThread implements Runnable {
//    private static class TaskThread extends Thread {
        CyclicBarrier barrier;
        public TaskThread(CyclicBarrier barrier) {
            this.barrier = barrier;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(1000);
                System.out.println(Thread.currentThread().getName() + " 到达栅栏 A");
                barrier.await();
                System.out.println(Thread.currentThread().getName() + " 冲破栅栏A");

                Thread.sleep(2000);
                System.out.println(Thread.currentThread().getName() + " 到达栅栏 B");
                barrier.await();
                System.out.println(Thread.currentThread().getName() + " 冲破栅栏 B");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
