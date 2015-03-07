package com.playground;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Persistence {

	public static void main(String[] args) {
		
		JavaSparkContext sc =  new JavaSparkContext("local","Persistence example");
		JavaRDD<String> name = sc.parallelize(Arrays.asList("guru","prasad","bobbi"));
		name.persist(StorageLevel.DISK_ONLY());
		System.out.println(name.count());
		
		JavaRDD<String> firstName = name.filter(new Function<String, Boolean>() {
			
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return v1.contains("guru");
			}
		});
		
		firstName.persist(StorageLevel.MEMORY_ONLY());  // default behaviour of storing on JVM heap in non serialized form
		JavaRDD<Integer> temp = sc.parallelize(Arrays.asList(1,2,3,4));
		temp.persist(StorageLevel.MEMORY_ONLY_SER()); // explicit storage on heap in serialized form
		System.out.println(firstName.first());
	}
}
