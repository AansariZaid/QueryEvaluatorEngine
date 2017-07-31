package com.stackroute.queryengine;

import java.util.ArrayList;
import java.util.Map;

public class QueryEngine {

	private QueryParameter queryParameter = new QueryParameter();
	private Map<Integer, ArrayList<String>> dataSet;
	private QueryProcessor queryProcessor = new QueryProcessor();
//	private CSVFileReader csvFileReader = new CSVFileReader();
	
	public Map<Integer,ArrayList<String>> executeQuery(String query)
	{
		queryParameter = queryParameter.extractParameter(query);
		
		System.out.println(queryParameter);
		
		dataSet = queryProcessor.processQuery(queryParameter);
		
		return dataSet;
	}
}
