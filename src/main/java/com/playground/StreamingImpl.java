package com.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingImpl {

	public static void main(String[] args) throws Exception {
		
		String host = "";
		String port = "";
		if(args.length != 2) {
			throw new Exception("Run as => spark-submit --master yarn --class com.playground.StreamingImpl spark-examples.jar quickstart.cloudera 9999");
		} else {
			host = args[0];
			port = args[1];
		}
		
		SparkConf conf = new SparkConf().setAppName("Streaming logs example");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//create a streaming context with 5 second interval 
		JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(5000));

		JavaDStream lines = jsc.socketTextStream(host, Integer.parseInt(port));
		
		JavaDStream errors = lines.filter(new Function<String, Boolean>() {

			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				return line.contains("error");
			}
		});
		
		errors.print();
		
		jsc.start();
		jsc.awaitTerminationOrTimeout(60 * 1000);
		jsc.stop();
	}
}
