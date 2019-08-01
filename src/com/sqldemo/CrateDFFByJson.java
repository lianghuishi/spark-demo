package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CrateDFFByJson {

    public static void main(String args[]){

        /**
         *  可以限制显示的行数 df.show(100);
         *  读取特定格式文件的另一种写法 DataFrame df = sqlContext.read().format("json").load("./jsonsql");
         *  不能读取嵌套的json格式
         */
        SparkConf conf = new SparkConf();
        conf.setAppName("json").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read().json("./jsonsql");
        df.show();
        df.printSchema();

        /**
         * 将 dataframe 注册成临时表 user
         * user 这张表补存在内存也不存在磁盘当中，相当于是一个指针指向源文件，底层操作解析 sparkjob 读取源文件
         */
        df.registerTempTable("user");
        DataFrame df2 = sqlContext.sql("select a.name ,a.age from user a where a.age>13");
        df2.show();
        df2.printSchema();


        //DataFrame可以转换成RDD，读取每一行的数据
        JavaRDD<Row> rowJavaRDD = df.javaRDD();
        rowJavaRDD.foreach(new VoidFunction<Row>() {
            private static final long serialVersionUID = 942620229343435190L;
            @Override
            public void call(Row row) throws Exception {
                //显示一行
                //System.out.println(row);
                //显示一行中的第一列
                System.out.println(row.get(0));
                //System.out.println((String) row.getAs("age"));
                //System.out.println((String) row.getAs(0));
            }
        });




        sc.stop();

    }

}
