package com.stackroute.queryengine.processors;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.stackroute.queryengine.CSVFileReader;
import com.stackroute.queryengine.QueryParameter;

public final class OrderByProcessor implements Processor  {

	private CSVFileReader csvFileReader = new CSVFileReader();
	private BufferedReader bufferedReader = null;
	
	private Map<Integer, ArrayList<String>> dataSet = null;
	private int orderByColumn;
	private SimpleProcessor simpleProcessor = new SimpleProcessor();
	
	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter) {
		
		dataSet = simpleProcessor.executeQuery(queryParameter);
		orderByColumn = queryParameter.getGroupByIndexInMap();
		
		List<Map.Entry<Integer, ArrayList<String>>> entries = new ArrayList<Map.Entry<Integer, ArrayList<String>>>(
				dataSet.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<Integer, ArrayList<String>>>() {
			@Override
			public int compare(Entry<Integer, ArrayList<String>> firstRow,
					Entry<Integer, ArrayList<String>> secondRow) {
				try {
					Double value1 = Double.parseDouble(firstRow.getValue().get(orderByColumn));
					Double value2 = Double.parseDouble(secondRow.getValue().get(orderByColumn));
					return value1.compareTo(value2);
				} catch (Exception e) {
					return firstRow.getValue().get(orderByColumn).compareTo(secondRow.getValue().get(orderByColumn));
				}
			}
		});
		
		Map<Integer, ArrayList<String>> sortedMap = new LinkedHashMap<Integer, ArrayList<String>>();
		for (Map.Entry<Integer, ArrayList<String>> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		
		return sortedMap;
	}

}
