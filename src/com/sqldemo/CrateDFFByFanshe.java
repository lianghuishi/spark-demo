package com.sqldemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 反射创建 dataFrame
 */
public class CrateDFFByFanshe {

    public static void main(String[] args) {

        Person p = new Person();

        SparkConf conf = new SparkConf();
        conf.setAppName("sql").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("./person");

        JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {
            private static final long serialVersionUID = 909546969170010532L;
            @Override
            public Person call(String line) throws Exception {
                String[] split = line.split(",");
                p.setId(split[0]);
                p.setName(split[1]);
                p.setAge(Integer.valueOf(split[2]));
                return p;
            }
        });

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(personRDD, Person.class);

        df.registerTempTable("person");
        DataFrame df2 = sqlContext.sql("select * from person where id = 'a11' or id = 'a12'");
        df2.show();

        //将dataFrame转换成RDD
        JavaRDD<Row> rowJavaRDD = df2.javaRDD();
        JavaRDD<Person> javaRDD1 = rowJavaRDD.map(new Function<Row, Person>() {
            private static final long serialVersionUID = -5050461281811038236L;
            @Override
            public Person call(Row row) throws Exception {
                //用get()方法注意：012的位置并不一定是文件里面数据的位置，是要看dataFrame打印出来的位置！！
                p.setId(row.getAs("id")+"");
                p.setName(row.getAs("name")+"");
                p.setAge(Integer.valueOf(row.getAs("age")+""));
                return p;
            }
        });
        javaRDD1.foreach(new VoidFunction<Person>() {
            private static final long serialVersionUID = -4603328131252074628L;
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

    }
}
