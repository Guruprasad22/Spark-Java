package com.playground;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.broadcast.Broadcast;


public class BroadcastImpl {
	
	public static void main(String[] args) throws Exception {
		
		String cluster = "";
		if(args.length != 1) {
			throw new Exception("Usage com.playground.BroadcastImpl <cluster-name>");
		} else {
			cluster = args[0];
		}
		
		JavaSparkContext sc =  new JavaSparkContext(cluster,"Broadcast example");
		final Broadcast<String []> callSingsBroadcast = sc.broadcast(loadCallSignTable());
	}
	
	static String[] loadCallSignTable() throws FileNotFoundException {
		
		Scanner scanner = new Scanner(new File("callsign_tbl_sorted"));
		ArrayList<String> callSigns = new ArrayList<String>();
		while(scanner.hasNextLine()) {
			callSigns.add(scanner.nextLine());
		}
		return callSigns.toArray(new String[0]);
	}
}
