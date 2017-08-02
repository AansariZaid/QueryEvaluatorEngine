package com.stackroute.queryengine.processors;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.queryengine.AggregateFunctions;
import com.stackroute.queryengine.CSVFileReader;
import com.stackroute.queryengine.QueryParameter;

public class AggregateProcessor implements Processor {

	private CSVFileReader csvFileReader = new CSVFileReader();
	private BufferedReader bufferedReader = null;
	private String line = null;
	private String tableName = null;
	private String[] lineData = null;
	private String[] headers = null;
	private Map<Integer, ArrayList<String>> dataSet = null;
	private ArrayList<String> rowData = null;
	private ArrayList<AggregateFunctions> aggregateList = null;
	private ArrayList<Integer> aggregateIndexes = new ArrayList<>();

	Double count = 0.0;
	Double value1 = 0.0;
	Double value2 = 0.0;
	Double result = 0.0;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter) {

		tableName = queryParameter.getTableName();
		headers = csvFileReader.fetchHeader(tableName);
		aggregateList = queryParameter.getAggregateFunctions();
		int rows = 0;
		
		for (AggregateFunctions element : aggregateList) {
			aggregateIndexes.add(csvFileReader.getIndex(element.getColumName(), headers));
		}
		dataSet = new LinkedHashMap<>();
		int rowCount = 0;

		try {
			bufferedReader = csvFileReader.bufferedReader;
			while ((this.line = bufferedReader.readLine()) != null) {
				rows++;
				lineData = line.split(",");
				int counter = 0;
				for (AggregateFunctions elements : aggregateList) {
					elements.setValue(evaluateAggregate(elements, lineData[aggregateIndexes.get(counter)]));
					counter++;
				}
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}

		for (AggregateFunctions elements : aggregateList) {
			System.out.println(elements);
			rowData = new ArrayList<>();
			rowData.add(elements.getFunctionName());
			rowData.add(elements.getColumName());

			if (elements.getFunctionName().equalsIgnoreCase("avg")) {
				double average = (elements.getValue()) / rows;
				rowData.add((Double.toString(average)));
			} else {
				rowData.add((Double.toString(elements.getValue())));
			}
			dataSet.put(rowCount, rowData);
			rowCount++;
		}
		return dataSet;
	}

	private Double evaluateAggregate(AggregateFunctions function, String data) {

		switch (function.getFunctionName()) {
		case "sum":
			try {
				value1 = Double.parseDouble(data);
				value2 = function.getValue();
				result = value1 + value2;

			} catch (Exception e) {
				e.printStackTrace();
			}
			break;

		case "count":
			result++;
			break;

		case "min":
			value1 = Double.parseDouble(data);
			value2 = function.getValue();

			if (value2 == 0.0) {
				// result = value1;
				result = value2 = value1;
			} else if (value1 < value2) {
				result = value1;
			}
			break;

		case "max":
			value1 = Double.parseDouble(data);
			value2 = function.getValue();

			if (value2 == 0.0) {
				result = value2 = value1;
			} else if (value1 > value2) {
				result = value1;
			}
			break;

		case "avg":
			count++;
			try {
				value1 = Double.parseDouble(data);
				value2 = function.getValue();
				result = (value1 + value2);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;

		default:
			break;
		}

		return result;

	}

}
