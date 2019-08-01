package com.streaming;

import com.google.common.base.Optional;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 *  Driver的HA 在提交application的时候 添加 --supervise 选项 如果Driver挂掉 会自动启动一个Driver
 *  但是如何保证让新的driver知道之前宕掉的driver处理的任务进度以及在kafka的消费偏移量等等呢？
 *   就需要利用checkpont 恢复程序 （JavaStreamingContext.getOrCreate(checkpointDir, factory);）
 *
 *	利用checkpont 恢复程序在上次消费kafka的偏移量
 *  利用	 updateStateByKey 做累计计算
 */
public class SparkStreamingOnHDFS {
	public static void main(String[] args) {
		final SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStreamingOnHDFS2");
		final String checkpointDir = "./checkpoint";

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContex(checkpointDir, conf);
			}
		};
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDir, factory);
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}


	private static JavaStreamingContext createContex(String checkpointDir,SparkConf conf1){

		/**
		 * 只会在第一次运行时候打印，之后都是直接从checponit恢复spark程序
		 */
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");
		System.out.println("create new context****************************");


		/**
		 * checkpoint保存：
		 * 		1.配置信息
		 * 		2.DStream操作逻辑
		 * 		3.job的执行进度
		 * 		4.offset
		 * 	（如果保存有checkpoint，那么下次运行的时候就直接运行Spark保存好的程序，想上面的几行字就不会打印出来，因为和spark的api无关）
		 */
		SparkConf sparkConf = conf1;

		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		jsc.checkpoint(checkpointDir);
		Map<String, String> kafkaParameters = new HashMap<>();
		kafkaParameters.put("metadata.broker.list", "mini1:9092,mini2:9092,mini3:9092");

		HashSet<String> topics = new HashSet<>();
		topics.add("aaaa");
		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(jsc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParameters,
				topics);

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() { //如果是Scala，由于SAM转换，所以可以写成val words = lines.flatMap { line => line.split(" ")}
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(Tuple2<String,String> tuple) throws Exception {
				return Arrays.asList(tuple._2.split("\t"));
			}
		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<>(word, 1);
			}
		});

		/*JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { //对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			private static final long serialVersionUID = 1L;
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});*/

        JavaPairDStream<String, Integer> wordsCount =
                pairs.updateStateByKey(new Function2<List<Integer>, com.google.common.base.Optional<Integer>, com.google.common.base.Optional<Integer>>() {
                    private static final long serialVersionUID = -1738559543975630059L;
                    @Override
                    public com.google.common.base.Optional<Integer> call(List<Integer> values, com.google.common.base.Optional<Integer> state) throws Exception {
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

		wordsCount.print();

		return jsc;


	}
}
