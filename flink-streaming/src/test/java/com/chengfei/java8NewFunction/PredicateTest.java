package com.chengfei.java8NewFunction;

import java.awt.*;
import java.util.function.Predicate;

/**
 * @ClassName: PredicateTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 10:09
 * @Version 1.0
 * predicate是用来做判断的它有一个test方法
 **/
public class PredicateTest {
    public static void main(String[] args) {
        Predicate<Integer> predicate = new Predicate<Integer>() {
            @Override
            public boolean test(Integer integer) {
                if (integer > 5) {
                    return true;
                }
                return false;
            }
        };

        System.out.println(predicate.test(6));

        System.out.println("用lambda表达式");
        predicate = t -> t > 5;
        System.out.println(predicate.test(1));
    }
}
