package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("./test2");
        //JavaRDD<String> line = sc.textFile("./test2",2);
        JavaRDD<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            public static final long serialVersionUID = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(",");
                List<String> strings = Arrays.asList(split);
                return strings;
            }
        });

        /*JavaRDD<String> map = line.map(new Function<String, String>() {
            private static final long serialVersionUID = -3134700704626513870L;
            @Override
            public String call(String line) throws Exception {
                return line;
            }
        });*/

        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 8695312645163954636L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -4515301411289476201L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //几个partition就有几个文件
        //reduceByKey.saveAsTextFile("./testfile/test"+reduceByKey.partitions().size());
        reduceByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = -3428783749731527442L;
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });


        //测试分区数据的排序：排序仅仅对分区内的数据进行排序
        /*JavaPairRDD<Integer, String> pairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 8347970216836758854L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        });
        JavaPairRDD<Integer, String> sortByKey = pairRDD.sortByKey();
        sortByKey.saveAsTextFile("./testfile/test");
        sortByKey.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            private static final long serialVersionUID = -3428783749731527442L;
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2);
            }
        });*/

    }

}
