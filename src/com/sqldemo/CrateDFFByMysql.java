package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *  读取mysql创建DataFrame 有两种方式，两种方式用哪一种都可以
 */
public class CrateDFFByMysql {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("json").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String,String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://mini3:3306/test");
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "root");
        options.put("dbtable", "user");
        DataFrame dataFrame = sqlContext.read().format("jdbc").options(options).load();
        dataFrame.registerTempTable("user");

        /*DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", "jdbc:mysql://mini3:3306/test");
        reader.option("driver", "com.mysql.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "root");
        reader.option("dbtable", "user");
        DataFrame dataFrame1 = reader.load();
        dataFrame1.registerTempTable("user1");*/

        DataFrame sql = sqlContext.sql("select * from user");
        sql.show();

        //将数据保存到mysql的表中
        /*Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "root");
        sql.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://mini3:3306/test", "user_copy", properties);
        */
        sc.close();
    }
}
