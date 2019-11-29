package com.chengfei.java8NewFunction;

import java.sql.SQLOutput;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @ClassName: SupplierTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 9:59
 * @Version 1.0
 * supplier相当于一个容器
 **/
public class SupplierTest {
    public static void main(String[] args) {
        Supplier<Integer> supplier = new Supplier<Integer>() {
            @Override
            public Integer get() {
                return new Random().nextInt();
            }
        };
        System.out.println(supplier.get());

        System.out.println("用lambda表达式实现supplier接口");
        Consumer<String> runnable = s -> new Random().nextInt();

        supplier = () -> new Random().nextInt();
        System.out.println(supplier.get());
    }
}
