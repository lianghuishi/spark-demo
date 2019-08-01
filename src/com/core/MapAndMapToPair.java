package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class MapAndMapToPair {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));

        line1.foreach(new VoidFunction<String>(){
            private static final long serialVersionUID = 8766858657622281019L;
            @Override
            public void call(String num) throws Exception {
                System.out.println("numbers;"+num);
            }
        });

        JavaPairRDD<String, String> prdd = line1.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = 6399646332434959593L;
            public Tuple2<String, String> call(String x) throws Exception {
                return new Tuple2(x.split(" ")[0], x.split(" ")[1]);
            }
        });
        System.out.println("111111111111mapToPair:");
        prdd.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long serialVersionUID = -5968345773345283451L;
            public void call(Tuple2<String, String> x) throws Exception {
                System.out.println(x);
            }
        });

       /* JavaRDD => JavaPairRDD: 通过mapToPair函数
        JavaPairRDD => JavaRDD: 通过map函数转换*/
        System.out.println("===============1=========");
        JavaRDD<String> javaprdd =prdd.map(new Function<Tuple2<String,String>,String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Tuple2<String, String> arg)  {
                // TODO Auto-generated method stub
                System.out.println("arg======================"+arg);
                System.out.println("arg======================"+arg._1);
                return arg._1+" "+arg._2;
            }

        });

        System.out.println("===============2=========");
        javaprdd.foreach(new VoidFunction<String>(){
            private static final long serialVersionUID = 5890676835445516835L;
            @Override
            public void call(String num) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("numbers;"+num);
            }
        });

    }
}
