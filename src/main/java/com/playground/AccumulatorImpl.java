package com.playground;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class AccumulatorImpl {
	
	public static void main(String[] args) throws Exception {
		
		String clusterName = "";
		String path = "";
		
		if(args.length < 2) {
			throw new Exception("Usage com.playground.AccumulatorImpl <cluster-name> <inputfile-path>");
		} else {
			clusterName = args[0];
			path = args[1];
		}
		
		JavaSparkContext sc = new JavaSparkContext(clusterName,"Accumulator example");
		JavaRDD<String> rdd = sc.textFile(path);
		final Accumulator<Integer> blankLineCount = sc.accumulator(0);
		JavaRDD<String> nonBlanks = rdd.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String line) throws Exception {
				if(line.isEmpty() || line == "") {
					blankLineCount.add(1);;
				} 
				return Arrays.asList(line.split(" "));
			}
		});
		System.out.println("Number of non blanks " + nonBlanks.count());
		System.out.println("Number of blank lines = " + blankLineCount);		
	}
	

}
