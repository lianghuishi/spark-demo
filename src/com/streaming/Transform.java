package com.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Transform {

    public static void main (String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("transform");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //黑名单
        List<String> bcBlackList = Arrays.asList("zhangshan");

        //接收socket数据源
        JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("192.168.58.140", 9999);

        JavaPairDStream<String, String> mapToPair = nameList.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = 7122366507842911022L;
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0],s);
            }
        });

        /**
         * transform可以拿到DStream中的RDD，做RDD到RDD之间的转换，不需要Action算子触发，需要返回RDD类型
         * 注意，transform 可以拿到DStream中的RDD call方法内，拿到RDD 算子外的代码在Driver端执行，也可以做到动态改变广播变量
         */
        JavaDStream<String> transform = mapToPair.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            private static final long serialVersionUID = -6541193375071992135L;
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> nameRDD) throws Exception {

                JavaPairRDD<String, String> filter = nameRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    private static final long serialVersionUID = -4980259252620553979L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple2) throws Exception {
                        boolean contains = bcBlackList.contains(tuple2._1);
                        return !contains;
                    }
                });

                JavaRDD<String> map = filter.map(new Function<Tuple2<String, String>, String>() {
                    private static final long serialVersionUID = 7492906521378365242L;
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        return tuple2._2;
                    }
                });

                return map;
            }
        });

        transform.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();

    }
}
