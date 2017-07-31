package com.stackroute.queryengine;

import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;

public class QueryTester {

	public static void main(String[] args) {

		Map<Integer, ArrayList<String>> dataSet;
		String query;
		QueryEngine queryEngine = new QueryEngine();
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter Query");
		query = sc.nextLine();

		dataSet = queryEngine.executeQuery(query);
		for (Map.Entry<Integer, ArrayList<String>> m : dataSet.entrySet()) {
			ArrayList<String> data = m.getValue();
			for (String s : data) {
				System.out.print(s + "\t");
			}
			System.out.println();
		}
	}

}
