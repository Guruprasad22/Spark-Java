package com.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Streaming {
	public static void main(String[] args) throws Exception {
		
		String clusterName = "";
		int duration = 1000;
		if(args.length != 2) {
			throw new Exception("Usage com.playground.Streaming <cluster-name> <sample-interval-in-seconds>");
		} else {
			clusterName = args[0];
			duration = Integer.parseInt(args[1]);			
		}
		
		SparkConf sc = new SparkConf();
		sc.setAppName("Streaming example");
		sc.setMaster(clusterName);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaStreamingContext jssc = new JavaStreamingContext(jsc,new Duration(1000 * duration));
		JavaDStream<String> log = jssc.socketTextStream("localhost", 7777);
		JavaDStream<String> errorLines = log.filter(new Function<String,Boolean>() {

			public Boolean call(String v1) throws Exception {
				return v1.contains("error");
			}});
		errorLines.print();
		
		jssc.start();
		jssc.awaitTermination(1000*100);
		jssc.stop();
		
	}
}
