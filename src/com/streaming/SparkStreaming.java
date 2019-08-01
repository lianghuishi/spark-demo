package com.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 *  nc -lk 9999
 */
public class SparkStreaming {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("tt").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        //sc.setLogLevel("WARN");

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

        JavaReceiverInputDStream<String> line = jsc.socketTextStream("192.168.58.140", 9999);

        JavaDStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 8953144946921285165L;
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });

        JavaPairDStream<String, Integer> mapToPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -6033897741353796814L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 8511314394403825469L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //仅仅显示的话可以用reduceByKey.print();代替
        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 5701405607290183625L;
            @Override
            public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                System.out.println("Driver....."); //在Driver端运行

                /*SparkContext context = pairRDD.context();
                JavaSparkContext javaSparkContext = new JavaSparkContext(context);
                //因为程序是不停的,这里可以通过动态改变广播变量，实现指定的业务功能，例如添加黑名单等
                Broadcast<String> broadcast = javaSparkContext.broadcast("hello");
                String value = broadcast.value();
                System.out.println("广播变量测试....."+value);*/

                JavaPairRDD<String, Integer> pairRDD1 = pairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    private static final long serialVersionUID = 5439019711021546733L;
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println("Executor....."); //在Executor端运行
                        return new Tuple2<>(tuple2._1,tuple2._2);
                    }
                });
                pairRDD1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 6730359810965342896L;
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(tuple2);
                    }
                });
            }
        });

//        reduceByKey.print();
        jsc.start();
        jsc.awaitTermination();

    }

}
