package com.playground;

import io.netty.util.internal.StringUtil;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
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
		JavaSparkContext sc = new JavaSparkContext("local","Filter on a collection");
		new Transformation().filterOnCollection(sc);
		String path = "", keyword = "";
		if(args.length ==  2) {
			path = args[0];
			keyword = args[1];
		}
		
		new Transformation().filterAnExternalDataSource(path, keyword, sc);
		
		new Transformation().squareMe(sc);
		
	}
	/**
	 * function to filter on an existing collection
	 */
	public void filterOnCollection(JavaSparkContext sc) {
		
		
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
	
	public void filterAnExternalDataSource(String path,String keyword, JavaSparkContext sc) {
		final String finalKeyword = keyword;
		JavaRDD<String> file = sc.textFile(path);
		JavaRDD<String> filteredContent = file.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				return v1.contains(finalKeyword);
			}
		});
		
		for(String str : filteredContent.toArray()) {
			System.out.println(str);
		}
	}
	
	public void squareMe(JavaSparkContext sc) {
		
		JavaRDD<Integer> source = sc.parallelize(Arrays.asList(1,2,3));
		JavaRDD<Integer> squaredNumbers = source.map(new Function<Integer, Integer>() {

			public Integer call(Integer v1) throws Exception {
				return v1 * v1;
			}
		});
		
		System.out.println("the squared numbers are ." + StringUtils.join(squaredNumbers.collect(),","));
	}
	
}
