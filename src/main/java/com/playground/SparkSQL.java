package com.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.hive.HiveContext;


public class SparkSQL {
	
	public static void main(String[] args) throws Exception {
		
		String cluster = "";
		
		if(args.length != 1) {
			throw  new Exception("usage -> com.playrgound.SparkSQL <cluster-name> ");
		} else {
			cluster = args[0];
		}
		
		JavaSparkContext sc = new JavaSparkContext(cluster,"Spark SQL app");
		HiveContext hiveContext = new HiveContext(sc.toSparkContext(null));
		SchemaRDD rdd = hiveContext.sql("select * from sample_07");
 
		for(Row row : rdd.collect()) {
			System.out.println(row.toString());
		}		
	}
}
