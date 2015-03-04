package com.playground;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * class to run various transformations and actions using Java APIs
 * @author Guruprasad
 *
 */

public class Transformation implements Serializable{

	public static void main(String[] args) {
		
		new Transformation().filterOnCollection();
		
	}
	/**
	 * function to filter on an existing collection
	 */
	public void filterOnCollection() {
		
		JavaSparkContext sc = new JavaSparkContext("local","Filter on a collection");
		JavaRDD<String> names = sc.parallelize(Arrays.asList("guru","praveen","adya"));
		JavaRDD<String> filteredName = names.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				return v1.contains("praveen");
			}
		});
		System.out.println("Before filtering .. ");
		for(String str : names.toArray()) {
			System.out.println(str);
		}
		
		System.out.println("After filtering");
		for(String str : filteredName.toArray()) {
			System.out.println(str);
		}
	}
}
