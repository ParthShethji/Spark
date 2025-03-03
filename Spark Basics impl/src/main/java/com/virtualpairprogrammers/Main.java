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
//        JavaRDD<String> initialRDD = sc.textFile("Project/src/main/resources/subtitles/input.txt");
//
//        initialRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator()).foreach(value -> System.out.println(value));
//        sc.close();


//        Keyword Ranking
//        JavaRDD<String> initialRDD = sc.textFile("Project/src/main/resources/subtitles/input.txt");
//        JavaRDD<String> sentences = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
//        JavaRDD<String> removedBlackLines = sentences.filter(sentence -> sentence.trim().length()>0);
//        JavaRDD<String> words = removedBlackLines.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//        JavaRDD<String> removedBlackWords = words.filter(sentence -> sentence.trim().length()>0);
//
//        JavaRDD<String> interstingWords = removedBlackWords.filter(Util::isNotBoring);
//        JavaPairRDD<String, Long> pairRDD = interstingWords.mapToPair(word -> new Tuple2<>(word, 1L))
//                .reduceByKey((value1, value2) -> value1+value2);
//
//        JavaPairRDD<Long, String> invertedPair = pairRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
//                .sortByKey(false);
//        List<Tuple2<Long, String>> result = invertedPair.take(50);
//        result.forEach(System.out::println);
//
//        sc.close();
//        here we used take 10 hence foreach    worked here

//        ***  Sorting doesnt work with foreach   Coalese()
//        and it has nothing to do with partition and is giving output of each partition
//        then waht we could  do is let it load into single partition and then apply foreach, Coalese() is used to make it all in 1 partition, but the problem is  that we migh get out of memory exception

//        The real reason is - foreach is given to each partition in parallel hence printline is executed on each node in parallel so both are running printline in parallel


//      Coalesce()
//        After performing many transformations (and maybe actions) on our multi Terabyte, multi partition RDD, we've now reached the point where we only have a small amount of data.
//                For the remaining transformations, there's no point in continuing across 1000 partitions - any shuffles will be pointlessly expensive
//        Coalesce is just a way of reducing the number of partitions - it is never needed
//        just to give the right answer. Virtual Pail Programmers


//        Collect()
//        collect() is generally used when you've finished and you want to gather a small  RDD onto the driver node for eg printing. Only call if you're sure the RDD will fit into a single JVM's RAM!
//        if the results are stiff "big", we'd write to a (eg HDFS) file


//      Action v/s Transformation
//        Action is where the real calculation of the RDD takes place its not just execution plan
//        rest all are just Transformations these all are lazly calculated
//        Spark can implement for eg filter transformation without moving any data around. For this reason it is called a "Narrow Transformation
//         then there is another transformation where shuffling is done this is called wide transformation
//         Make minimum number of shuffles in java


//        Shuffles
//        **** Note in ---- if stage 1 is taking more time than stage 0 then there is a problem with performance there is need to finding a better approach


//        Instead of using groupBy try using map-sid-reduce
//            Group by tries to keep all the same type data in one partition this can cause out of memoty error hence we gnerally avoid using it
//        even it has to do same thing but it will reduce the data that are to be shuffled

        // Caching and Persistence


//        When you get an action it has to go all the way initial RDD re run it doesn't store all the intermediate media but it has optimisation some actions again
//        we can use persist and use memory if want to store only in memory or can also be stored in disk








     }
}
