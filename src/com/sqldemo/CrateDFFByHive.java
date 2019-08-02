package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class CrateDFFByHive {

    public static void main(String args[]){

        SparkConf conf = new SparkConf();
        conf.setAppName("hive");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext hiveContext = new HiveContext(sc);

        hiveContext.sql("use test");
        hiveContext.sql("select * from t_dsb2").show();

        sc.stop();

    }

}
