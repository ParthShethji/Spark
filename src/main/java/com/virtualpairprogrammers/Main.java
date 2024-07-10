package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import  java.util.*;


public class Main {
    public static void main(String[] args) {
//        ArrayList<Integer> inputData = new ArrayList<>();
//        inputData.add(2);
//        inputData.add(8);
//        inputData.add(1);
//        inputData.add(7);
//        inputData.add(11);

        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        // means run spark in local conf we dont have a cluster and use all the available cause
        JavaSparkContext sc = new JavaSparkContext(conf);
        //connect to spark

//        JavaRDD<String> myRDD = sc.parallelize(inputData);
        //load data into spark

//Reduce
//        Integer res =myRDD.reduce(Integer::sum);
//          Integer res = myRDD.reduce((value1, value2 )-> value1+value2);
//        System.out.println(res);
//        Reduce is used when you want to perform some operation in big data and what it does is it gives this function to to each partition and in partition randomly 2 values are selected and function is applied to them then another reduce is called and it applies to other 2 values and this goes on.
//        Return type must same as input type



//Mapping
//        JavaRDD<Double> sqrtRDD = myRDD.map(Math::sqrt);
//        JavaRDD<Double> sqrtRDD = myRDD.map(value -> Math.sqrt(value));
//        sqrtRDD.foreach(value -> System.out.println(value));
//        sqrtRDD.collect().forEach(System.out::println);



//        count number of elements
//        System.out.println(sqrtRDD.count());

//        common trick used ot count
//        JavaRDD<Long> singleInt = myRDD.map(value -> 1L);
//        Long ans = singleInt.reduce(Long::sum);
//        System.out.println(ans);

//      How to keep these numbers in the same row as it will be easy for calc.
//      Tuple
//        JavaRDD<Tuple2<Integer, Double>> sqrtRDD = myRDD.map(value -> new Tuple2<>(value, Math.sqrt(value)));
//        you can go from 2 to 22




//        Pair RDD
//        JavaPairRDD<String, String> pairRDD = myRDD.mapToPair(value -> {
//            String[] column =value.split(":");
//            String level = column[0];
//            String date = column[1];
//
//            return new Tuple2<>(level, date);
//        });


//        count error without using groupby as it causes lot of problems
//        JavaPairRDD<String, Long> pairRDD = myRDD.mapToPair(value -> {
//            String[] column =value.split(":");
//            String level = column[0];
//            return new Tuple2<>(level, 1L);
//        });
//
//        JavaPairRDD<String, Long> sumRDD = pairRDD.reduceByKey((value1, value2)-> value1 + value2);
//        sumRDD.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

//      All the above can be done in
//        JavaPairRDD<String, Long> pairRDD = myRDD
//                .mapToPair(value -> new Tuple2<>(value.split(":")[0], 1L))
//                .reduceByKey((value1, value2 )-> value2+value1);
//
//        pairRDD.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));



//        FlatMaps - A map with multiple values
//        JavaRDD<String> words = myRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//        words.filter(word -> word.length()>1)
//        .collect().forEach(System.out::println);


//        Reading from a file
//        here the entire file is not stored but partition of that big data is loaded
        JavaRDD<String> initialRDD = sc.textFile("Project/src/main/resources/subtitles/input.txt");

        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator()).foreach(value -> System.out.println(value));
        sc.close();

    }
}
