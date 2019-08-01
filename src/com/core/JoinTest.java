package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JoinTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("afsad");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", "a"),
                new Tuple2<>("lisi", "b"),
                new Tuple2<>("wangwu", "c"),
                new Tuple2<>("maliu", "d")
        ));

        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", 100),
                new Tuple2<>("lisi", 200),
                new Tuple2<>("wangwu", 300),
                new Tuple2<>("maliu", 400)
        ));
        JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("zhangsan", "a2"),
                new Tuple2<>("lisi", "b2"),
                new Tuple2<>("wangwu", "c2"),
                new Tuple2<>("maliu", "d2")
        ));

        //join 需要两边都有值
        /*System.out.println("********************");
        JavaPairRDD<String, Tuple2<String, Integer>> join = rdd1.join(rdd2);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Integer>>>() {
            private static final long serialVersionUID = -9050430361297616525L;
            @Override
            public void call(Tuple2<String, Tuple2<String, Integer>> s) throws Exception {
                System.out.println(s);
            }
        });*/

        //union 把结果都合并（相当于list1后面追加list2的内容）
        /*System.out.println("********************");
        System.out.println(union.partitions().size());
        union.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long serialVersionUID = 4653761549930376985L;
            @Override
            public void call(Tuple2<String, String> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });*/

        //leftjoin
        /*System.out.println("********************");
        JavaPairRDD<String, Tuple2<String, Optional<Integer>>> leftJoin = rdd1.leftOuterJoin(rdd2);
        leftJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<Integer>>>>() {
            private static final long serialVersionUID = 4653761549930376985L;
            @Override
            public void call(Tuple2<String, Tuple2<String, Optional<Integer>>> s) throws Exception {
                System.out.println(s);
            }
        });*/

        //leftOuterJoin(leftjoin的简写)
        /*System.out.println("********************");
        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> pairRDD = rdd2.leftOuterJoin(rdd1);
        pairRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<String>>>>() {
            private static final long serialVersionUID = -7443498171193096365L;
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<String>>> s) throws Exception {
                System.out.println(s);
            }
        });*/

        //rightJoin
        /*System.out.println("********************");
        JavaPairRDD<String, Tuple2<Optional<String>, Integer>> rightJoin = rdd1.rightOuterJoin(rdd2);
        rightJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<String>, Integer>>>() {
            private static final long serialVersionUID = 7459824370640771260L;
            @Override
            public void call(Tuple2<String, Tuple2<Optional<String>, Integer>> s) throws Exception {
                System.out.println(s);
            }
        });*/


        /*leftJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Optional<Integer>>> s) throws Exception {
                String key = s._1;
                String value1 = s._2._1;
                Optional<Integer> optional = s._2._2;  //注意，这里的optional有可能为空，所以leftjoin的时候注意判空
            }
        });*/




    }
}
