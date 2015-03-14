package com.playground;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class AverageCount implements Serializable {
	
	int total;
	int count;
	
	public AverageCount(int total, int count) {
		super();
		this.total = total;
		this.count = count;
	}	
	
	public float avg() {
		return total/ (float) count;
	}	
	
	public static void main(String[] args) {
		// accumulator
		Function<Integer,AverageCount> accumulator = new Function<Integer, AverageCount>() {
			
			public AverageCount call(Integer x) throws Exception {
				// TODO Auto-generated method stub
				return new AverageCount(x,1);
			}
		};
		
		// combiner
		Function2<AverageCount,Integer,AverageCount> addUp =  new Function2<AverageCount, Integer, AverageCount>() {
			
			public AverageCount call(AverageCount current, Integer newVal) throws Exception {
				
				current.total = current.total + newVal;
				current.count = current.count + 1;			
				return current;
			}
		};
		
		// merge
		Function2<AverageCount,AverageCount,AverageCount> mergeAcrossPartitions = new Function2<AverageCount, AverageCount, AverageCount>() {
			
			public AverageCount call(AverageCount avg1, AverageCount avg2)
					throws Exception {
				// TODO Auto-generated method stub
				return new AverageCount(avg1.total + avg2.total, avg1.count + avg2.count);
			}
		};
		
		JavaSparkContext sc= new JavaSparkContext("local","average function");
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5));
		
		AverageCount initial = new AverageCount(0,0);
		AverageCount result = rdd.aggregate(initial, addUp, mergeAcrossPartitions);
	    System.out.println(result.avg());
	    sc.stop();		
	}	
}

