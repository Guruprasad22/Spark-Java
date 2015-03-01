package com.playground;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Frst spark app");
		conf.setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		String inputPath ="",outputPath = "";
		
		if(args.length == 2) {
			inputPath = args[0];
			outputPath = args[1];
		} else {
			inputPath = "/home/cloudera/bookmark.js";
			outputPath = "/home/cloudera/";
		}

		
		JavaRDD<String> text = context.textFile(inputPath);
		
		// flatMap Return a new RDD by first applying a function to all elements of this RDD,
		// and then flattening the results.
		
		JavaRDD<String> words = text.flatMap(
				new FlatMapFunction<String,String> () {

					public Iterable<String> call(String script) throws Exception {
						// TODO Auto-generated method stub
						return Arrays.asList(script.split(" "));
					}					
				});
		
		// Transform into pairs and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
		  new PairFunction<String, String, Integer>(){
		    public Tuple2<String, Integer> call(String x){
		      return new Tuple2(x, 1);
		    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
		        public Integer call(Integer x, Integer y){ return x + y;}});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputPath);		
	}
}
