package com.oreilly.learningsparkexamples.mini.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class BasicSum {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		Integer result = rdd.fold(1, new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				System.out.println("x:"+x+"|| y :"+y);
				return x + y;
			}
		});
		Integer reducedResult = rdd.reduce( new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				System.out.println("x:"+x+"|| y :"+y);
				return x + y;
			}
		});
		System.out.println(result);
		System.out.println("Reduced Result :"+reducedResult);
	}
}