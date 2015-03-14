package com.playground;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;

public class LoadJSON  {
	
	public static class Person implements Serializable {
		public String name;
		public String employed;
		public int experience;
		
		@Override
		public String toString() {
			return "Person [name=" + name + ", employed=" + employed
					+ ", experience=" + experience + "]";
		}
	}
	
	public static class ParseJSON implements FlatMapFunction<Iterator<String>, Person> {
		public Iterable<Person> call(Iterator<String> lines) throws Exception {			
			ArrayList<Person> people = new ArrayList<Person>();
			ObjectMapper mapper = new ObjectMapper();
			while(lines.hasNext()) {
				String line  = lines.next();
				people.add(mapper.readValue(line, Person.class));
			}
			return people;
		}		
	}
	
	public static class Employed implements Function<Person, Boolean> {
		public Boolean call(Person arg0) throws Exception {			
			return arg0.employed.equalsIgnoreCase("yes");
		}		
	}
	
	public static void main(String[] args) throws Exception {		
		String cluster = "",fileName = "";
		if(args.length != 2) {
			throw new Exception("Use it as LoadJSON ");
		} else {
			cluster =  args[0];
			fileName = args[1];
		}
		
		JavaSparkContext sc = new JavaSparkContext(cluster,fileName);
		JavaRDD<String> rdd = sc.textFile(fileName);
		
		JavaRDD<Person> employed = rdd.mapPartitions(new ParseJSON()).filter(new Employed());
				
		for(String s : rdd.toArray()) {
			System.out.println(s);
		}
		
		for(Person p: employed.toArray()) {
			System.out.println(p);
		}
	}


}
