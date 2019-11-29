package com.chengfei.java8NewFunction;


import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * @ClassName: ConsumerTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 9:38
 * @Version 1.0
 * 传一个值给它，它经过accept函数处理后给你们返回一个结果
 **/
public class ConsumerTest {
    public static void main(String[] args) {
        Consumer<String> consumer = new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        };

        Stream<String> stream = Stream.of("aaa", "bbb", "ddd", "ccc", "fff");
        stream.forEach(consumer);

        System.out.println("lambda表达式的使用");
        stream = Stream.of("ccc", "ddd");
        Consumer<String> tConsumer = s -> System.out.println(s);
        stream.forEach(tConsumer);

        Consumer c = System.out::println;

    }
}
