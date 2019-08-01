package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class OrderTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line = sc.textFile("./test2");

        JavaRDD<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 3312111669478878691L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(",");
                List<String> strings = Arrays.asList(split);
                return strings;
            }
        });

        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -5735899647032694845L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 5159161771420599495L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> pairRDD = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 4930152747690073893L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
//                return new Tuple2<>(tuple2._2,tuple2._1);
                return tuple2.swap();
            }
        });

        JavaPairRDD<Integer, String> pairRDD1 = pairRDD.sortByKey(false);

        JavaPairRDD<String, Integer> pairRDD2 = pairRDD1.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = -8552757168640652896L;
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        pairRDD2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 3402464284363342326L;
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
    }

}
