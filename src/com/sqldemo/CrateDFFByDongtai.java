package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * 动态创建 dataFrame   （与反射相比创建dataFrame不需要创建对应的javabean）
 */
public class CrateDFFByDongtai {

    public static void main(String[] args) {

        Person p = new Person();

        SparkConf conf = new SparkConf();
        conf.setAppName("sql").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("./person");

        JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {
            private static final long serialVersionUID = -7540687090770058758L;
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                return RowFactory.create(
                        split[0],
                        split[1],
                        Integer.valueOf(split[2])
                );
            }
        });

        List<StructField> structList = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        );
        StructType schema = DataTypes.createStructType(structList);

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);

        //下面可以通过 dataFrame 对数据进行一些列操作ss


        dataFrame.show();
        dataFrame.printSchema();

    }
}
