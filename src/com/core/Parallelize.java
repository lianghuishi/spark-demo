package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Parallelize {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("parallelize");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a","a","a","a","a","a");
        JavaRDD<String> javaRDD = sc.parallelize(list);
        //sc.parallelize(list, 4);//可以指定分区，默认分区是1，几个分区有几个partition，有几个task可以并行处理
        List<String> collect = javaRDD.collect();


        List<Tuple2<String,String>> list2 = Arrays.asList(
                new Tuple2<>("aa","11"),
                new Tuple2<>("aa","11"),
                new Tuple2<>("aa","11"));

        //不是k，v格式的rdd
        JavaRDD<Tuple2<String, String>> parallelize = sc.parallelize(list2);
        /*parallelize.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long serialVersionUID = 6765662051411640585L;
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {

            }
        });*/
        //转换成kv格式的rdd，相当于sc.parallelizePairs
        JavaPairRDD<String, String> javaPairRDD = parallelize.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            private static final long serialVersionUID = -2095237826674942849L;
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2;
            }
        });

        //k,v格式rdd
        JavaPairRDD<String, String> javaPairRDD2 = sc.parallelizePairs(list2);
        javaPairRDD2.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long serialVersionUID = 2775523708763292676L;
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {

            }
        });

        sc.close();

    }

}
