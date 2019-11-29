package com.chengfei.java8NewFunction;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @ClassName: FunctionTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 10:16
 * @Version 1.0
 **/
public class FunctionTest {
    public static void main(String[] args) {
        Function<String, Integer> function = new Function<String, Integer>() {
            @Override
            public Integer apply(String s) {
                return s.length();
            }
        };
        Stream<String> stream = Stream.of("aaa", "bbbbb", "ccccccv");
        Stream<Integer> integerStream = stream.map(function);
//        integerStream.forEach(s-> System.out.println(s));
        //流只能被消费一次
        integerStream.forEach(System.out::println);


    }
}
