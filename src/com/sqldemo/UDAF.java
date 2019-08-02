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
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class UDAF {

    public static void main(String args[]){

        SparkConf conf = new SparkConf();
        conf.setAppName("UDF").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //动态创建一个user列表
        createDataFrame(sc,sqlContext);

        //自定义一个udaf函数，实现统计相同值个数
        sqlContext.udf().register("StringCount", new UserDefinedAggregateFunction() {

            /**
             * 初始化一个内部自己定义的值，在Aggregate之前每组数据的初始化结果（第一个0表示行的位置，相当于row.getInt(0)）
             */
            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0,0);
            }

            /**
             *  更新可以认为一个个地将组内的字段值传递进来实现拼接的逻辑
             *  buffer.getInt（0）获取的是上一次聚合后的值
             *  相当于map的combiner，combiner就是每一个maptask的处理结果进行一次小聚合
             *  大聚合发生在reduce端
             *  这里既是：在进行聚合时，每当有新的值进来，对分组后的聚合如何进行计算
             */
            @Override
            public void update(MutableAggregationBuffer buffer, Row row) {
                buffer.update(0, buffer.getInt(0)+1);
            }

            /**
             *  合并update操作，可能是针对一个分组内的部分数据，在某一节点上发生的 但是可能是一个分组的数据，会分布在多个节点上
             *  buffer.getInt(0) 大聚合时候上次聚合后的值
             *  row.getInt(0) 这次计算传入进来上面update的结果（即小聚合的结果）
             */
            @Override
            public void merge(MutableAggregationBuffer buffer, Row row) {
                buffer.update(0, buffer.getInt(0)+row.getInt(0));
            }

            /**
             *  指定输入字段及类型
             */
            @Override
            public StructType inputSchema() {
                List<StructField> structList = Arrays.asList(
                        DataTypes.createStructField("nameXXX", DataTypes.StringType, true)
                );
                StructType structType = DataTypes.createStructType(structList);
                return structType;
            }

            /**
             *  在进行聚合操作的时候要处理的数据结果类型
             */
            @Override
            public StructType bufferSchema() {
                List<StructField> structList = Arrays.asList(
                        DataTypes.createStructField("nameXXX", DataTypes.IntegerType, true)
                );
                StructType structType = DataTypes.createStructType(structList);
                return structType;
            }

            /**
             *  最后返回一个和dataType方法类型要一致的类型，返回UDAF最后的计算结果
             */
            @Override
            public Object evaluate(Row row) {
                return row.getInt(0);
            }

            /**
             *  指定UDFA函数计算后返回的结果类型
             */
            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            /**
             * 确保一致性 一般用true 用来标记针对给定的一组输入，UDAF是否总是生成相同的结果
             */
            @Override
            public boolean deterministic() {
                return true;
            }

        });
        sqlContext.sql("select * from user").show();
        sqlContext.sql("select name , StringCount(name) as cc from user group by name").show();

    }

    private static void createDataFrame(JavaSparkContext sc, SQLContext sqlContext){
        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList(
                "zhangsan","zhangsan","zhangsan", "list","list", "wangwu", "maliu"));
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
