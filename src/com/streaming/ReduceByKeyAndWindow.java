package com.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 可以查询一段时间内的记录
 * 注意：
 *      没有优化的窗口函数可以不设置checkpoint目录(因为用不到)
 *      优化的窗口函数必须设置checkpoint目录
 */
public class ReduceByKeyAndWindow {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("tt").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> line = jsc.socketTextStream("192.168.58.140", 9999);

        JavaDStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 3339386630753398779L;
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });

        JavaPairDStream<String, Integer> mapToPair = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -5824594148615276432L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        /*JavaPairDStream<String, Integer> reduce =
                mapToPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -4487512700218583334L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, Durations.seconds(15), Durations.seconds(5));*/

        //window窗口优化操作
        jsc.checkpoint("./checkpoint");
        JavaPairDStream<String, Integer> reduce =
                mapToPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -4487512700218583334L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -953511680352722631L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1-v2;
                    }
                }, Durations.seconds(15), Durations.seconds(5));

        reduce.print();
        jsc.start();
        jsc.awaitTermination();

    }

}
