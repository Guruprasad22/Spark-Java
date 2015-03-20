package com.playground;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkProperties {
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("set spark properties");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("guru","praveen","adya"));
		System.out.println("Number fo elements : " + rdd.count());		
	}
}
