package com.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *  cache() 持久化算子,需要action算子触发才能将数据存到内存当中。
 *  rdd1.cache(); action算子触发之后，将rdd1的数据存储到内存中，
 *  如果下一个操作需要用到rdd1的数据，
 *  则直接可以从内存中读取到rdd1的数据，不用再次从文件中获取
 *
 *  persist()和checkpoint()的区别：
 *      persist()：application运行完数据直接回收，
 *      checkpoint()：application运行完数据依然存在磁盘中，下个application还是可以用;
 *      persist没有切断功能？？
 *      某些特定场景用checkpoint();
 */
public class CacheTest {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop");
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setCheckpointDir("./checkpoint");

        JavaRDD<String> rdd = sc.textFile("./test2");

        //rdd.cache();
        rdd.checkpoint();
        //rdd.persist(StorageLevel.MEMORY_ONLY()); //和rdd.cache();效果一样，不过persist可以设置_2来备份两份数据

        long t = System.currentTimeMillis();
        long count = rdd.count();  //action算子触发cache（）算子，将数据存到内存当中
        long t2 = System.currentTimeMillis();
        System.out.println("第一次结果："+count+"时间："+(t2-t));   //第一次运行action算子的时候会将数据从磁盘中读取，所以花费时间比较长。

        long t3 = System.currentTimeMillis();
        long count2 = rdd.count();
        long t4 = System.currentTimeMillis();
        System.out.println("第二次结果："+count+"时间："+(t4-t3));  // 第二次计算的时候因为数据是从内存中直接读取的，所以花费时间很短。

    }
}
