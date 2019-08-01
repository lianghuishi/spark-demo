package com.streaming;

import com.google.common.base.Optional;
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
import java.util.List;

/**
 *  nc -lk 9999
 * 设置checkpoint目录
 * 多久会将内存中的数据（每一个key所对应的状态）写入磁盘上一份呢
 * 如果你的 batchInterval 小于10s， 那么10s会将内存中的数据写入磁盘一份
 * 如果batchInterval大于10s,那么就以batchInterval为准
 * 这样做是为了防止频繁些hdfs
 */
public class UpdateStateByKey {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("tt").setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        jsc.checkpoint("./checkpoint");

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

        JavaPairDStream<String, Integer> counts =
                mapToPair.updateStateByKey(new Function2<List<Integer>,  Optional<Integer>,Optional<Integer>>() {
                    private static final long serialVersionUID = -1738559543975630059L;

                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        /**
                         * values:经过分组后（读取的新数据）这个key所对应的value列表
                         * state：这个key在本次之前的状态
                         */
                        Integer updateValue = 0;
                        if(state.isPresent()){
                            updateValue = state.get(); //如果之前的key是有值得，那么加上原来的key
                        }

                        for (Integer value: values) {
                            updateValue +=value;
                        }
                        return Optional.of(updateValue);
                    }
                });

        counts.print();
        jsc.start();
        jsc.awaitTermination();

    }

}
