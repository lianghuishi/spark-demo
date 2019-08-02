package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class UDF {

    public static void main(String args[]){

        SparkConf conf = new SparkConf();
        conf.setAppName("UDF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //动态创建一个user列表
        createDataFrame(sc,sqlContext);

        //自定义一个udf函数，并使用。DataTypes.IntegerType 和UDF1里面输出的类型保持一致
        sqlContext.udf().register("Strlen", new UDF1<String, Integer>() {
            private static final long serialVersionUID = 632475877208994179L;
            @Override
            public Integer call(String s) throws Exception {
                return  s.length();
            }
        }, DataTypes.IntegerType);
        sqlContext.sql("select * from user").show();
        sqlContext.sql("select name , Strlen(name) as ll from user").show();

    }

    private static void createDataFrame(JavaSparkContext sc, SQLContext sqlContext){
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("zhangsan", "list", "wangwu", "maliu"));
        JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {
            private static final long serialVersionUID = 942620229343435190L;
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });
        List<StructField> structList = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(structList);
        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);
        dataFrame.registerTempTable("user");
    };

}
