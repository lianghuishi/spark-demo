package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;


/**
 * foreach()和map()都是一条条处理数据，不过foreach()是action算子，map()是transformation 可以返回rdd
 *
 * foreachPartitions()和mapPartitions()是批次处理分区里面的数据，一个分区一个分区的处理
 */
public class MapPartitions {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("afsad");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", "a1"),
                new Tuple2<>("lisi", "b"),
                new Tuple2<>("hh", "c"),
                new Tuple2<>("bb", "d"),
                new Tuple2<>("maliu", "e")
        ),2);

       rdd1.map(new Function<Tuple2<String, String>, String>() {
           private static final long serialVersionUID = -1503293991840831729L;
           @Override
            public String call(Tuple2<String, String> s) throws Exception {
                System.out.println("map创建数据库连接------------");
                return "";
            }
        }).collect();

        rdd1.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, String>() {
            private static final long serialVersionUID = 8275493353481275019L;
            @Override
            public Iterable<String> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                System.out.println("mapPartitions创建数据库连接------------");
                return new ArrayList<>();
            }
        }).collect();


    }
}
