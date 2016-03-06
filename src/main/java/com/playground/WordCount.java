package com.playground;

import java.util.Arrays;
/**
 * word count example using geeky transformation methods
 */
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		String inputPath = "";
		String outputPath = "";
		String tokenizer = "";
		
		if(args.length != 3) {
			System.out.println("You passes " + args.length + " arguments");
			throw new Exception("Usage => spark-submit --class com.playground.WordCount spark-examples.jar <input-file-path> <output-path> <tokenizer>");
		} else {
			inputPath = args[0];
			outputPath = args[1];
			tokenizer = args[2];
		}
		
		SparkConf conf = new SparkConf();
		conf.setAppName("word count example");
		JavaSparkContext sc = new JavaSparkContext(conf); // this will be our handle
		
		JavaRDD<String> text = sc.textFile(inputPath);
		
		// flatMap returns a new RDD by first applying a function to all elements of this RDD,
		// and then flattening the results.
		final String tokenizerConstant = tokenizer;
		JavaRDD<String> words = text.flatMap(
				new FlatMapFunction<String,String> () {
					public Iterable<String> call(String script) throws Exception {
						// TODO Auto-generated method stub
						return Arrays.asList(script.split(tokenizerConstant));
					}					
				});
		
		// Transform into pairs and count.
		JavaPairRDD<String, Integer> counts = words.mapToPair(
		  new PairFunction<String, String, Integer>(){
		    public Tuple2<String, Integer> call(String x){
		      return new Tuple2(x, 1); // creates a tuple for each word
		    }}).reduceByKey(new Function2<Integer, Integer, Integer>(){
		        public Integer call(Integer x, Integer y){ return x + y;}}); // adds all tuple values for a given key
		// Save the word count back out to a text file, causing evaluation of lineage graph.
		counts.saveAsTextFile(outputPath);		
	}
}
