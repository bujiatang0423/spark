package org.simple.spark.hello;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

/**
 * @author bujiatang
 */
public class HelloWord {
    public static void main(String[] args) {
        String filePath = Objects.requireNonNull(HelloWord.class.getClassLoader()
                .getResource("hello/wikiOfSpark.txt")).getPath();

        final SparkSession spark = SparkSession.builder()
                .appName("SparkFileRead-Java")
                .master("local[*]")
                .getOrCreate();

        final JavaRDD<String> javaRDD = spark.sparkContext().textFile(filePath, 1).toJavaRDD();

        javaRDD.take(5).forEach(System.out::println);

        spark.stop();
    }
}
