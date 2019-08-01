package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 *
 */
public class CrateDFFByParquent {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("json").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json("./jsonsql");



    }
}
