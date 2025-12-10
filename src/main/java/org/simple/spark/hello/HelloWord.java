package org.simple.spark.hello;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
        final JavaRDD<String> wordRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        final JavaRDD<String> filter = wordRDD.filter(word -> !word.isEmpty());

        final JavaPairRDD<String, Integer> pairRDD = filter.mapToPair(word -> new Tuple2<>(word, 1));

        final JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(Integer::sum);

        final List<Tuple2<Integer, String>> take = countRDD.mapToPair(tp -> new Tuple2<>(tp._2, tp._1)).sortByKey(false).take(5);

        take.forEach(System.out::println);

        spark.stop();
    }
}
