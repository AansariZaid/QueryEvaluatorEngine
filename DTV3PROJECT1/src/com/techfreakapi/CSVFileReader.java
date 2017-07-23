package com.techfreakapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CSVFileReader {
	private BufferedReader bufferedReader;
	private String line = "";
	private String[] headerColumn;
	private double value;
	private double lineValue;

	// ***********************************METHOD TO FETCH HEADERS FROM
	// FILE****************////
	String[] fetchHeader(String tableName) {
		try {
			bufferedReader = new BufferedReader(new java.io.FileReader("E:\\" + tableName + ".csv"));
			line = bufferedReader.readLine();
			headerColumn = line.split(",");
		} catch (IOException e) {
			System.out.println("Error While Fetching Header");
		}
		return headerColumn;
	}
	
	// **************METHOD TO FETCH DATA WITHOUT WHERE CLASUES*****************///
	public Map<Integer, ArrayList<String>> readData(String[] selectedColumns, String[] headers) {
		Map<Integer, ArrayList<String>> rowSet = new LinkedHashMap<>();
		int rowCount = 0;
		int columnLength = selectedColumns.length;
		int headerLength = headers.length;
		try {
			while ((this.line = bufferedReader.readLine()) != null) {
				String[] lineData = line.split(",");
				ArrayList<String> rowData = new ArrayList<>();
				for (int i = 0; i < columnLength; i++) {
					for (int j = 0; j < headerLength; j++) {
						if (selectedColumns[i].trim().equalsIgnoreCase(headers[j].trim())) {
							rowData.add(lineData[j].trim());
						}
					}
				}
				rowSet.put(Integer.valueOf(rowCount), rowData);
				rowCount++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rowSet;
	}
	// ************************METHOD TO FETCH INDEXES OF SELECTIVE
	// COLUMNS************/
	public ArrayList<Integer> getIndexes(String[] selectedColumns, String[] headers) {
		int columnLength = selectedColumns.length;
		int headerLength = headers.length;
		ArrayList<Integer> indexes = new ArrayList<>();
		for (int i = 0; i < columnLength; i++) {
			for (int j = 0; j < headerLength; j++) {
				if (selectedColumns[i].trim().equalsIgnoreCase(headers[j].trim())) {
					indexes.add(Integer.valueOf(j));
				}
			}
		}
		return indexes;
	}

	/************** Method to read single index ************/
	private int getIndex(String selectedColumn, String[] headers) {

		int headerLength = headers.length;
		int index = -1;
		for (int j = 0; j < headerLength; j++) {
			if (selectedColumn.trim().equalsIgnoreCase(headers[j].trim())) {
				index = Integer.valueOf(j);
			}
		}
		return index;
	}

	// ************************METHOD TO FETCH INDEXES OF WHERE CLAUSE
	// COLUMNS************////////
	private ArrayList<Integer> getWhereClauseIndexes(ArrayList<Criteria> whereClause, String[] headers) {
		ArrayList<Integer> indexes = new ArrayList<>();
		for (Criteria criteria : whereClause) {
			for (int i = 0; i < headers.length; i++) {
				if (criteria.getColumnName().trim().equalsIgnoreCase(headers[i])) {
					indexes.add(i);
				}
			}
		}
		return indexes;
	}

	// ************************METHOD TO READ DATA WITH WHERE
	// CLAUSES************////////
	Map<Integer, ArrayList<String>> readData(String[] selectedColumns, String[] headers,
			ArrayList<Criteria> whereClause, ArrayList<String> whereConditions) {
		Map<Integer, ArrayList<String>> rowSet = new LinkedHashMap<>();
		int rowCount = 0;
		ArrayList<Integer> indexes = getIndexes(selectedColumns, headers);
		ArrayList<Integer> whereIndexes = getWhereClauseIndexes(whereClause, headers);
		int whereConditionSize = whereConditions.size();
		try {
			while ((this.line = bufferedReader.readLine()) != null) {
				String[] lineData = line.split(",");
				ArrayList<String> rowData = new ArrayList<>();
				ArrayList<Boolean> flags = new ArrayList<>();
				boolean flag = false;
				if (whereConditionSize > 0) {
					for (int counter = 0; counter <= whereConditionSize; counter++) {
						flags.add(evaluateWhereCondition(((Criteria) whereClause.get(counter)).getOperator(),
								((Criteria) whereClause.get(counter)).getValue(),
								lineData[((Integer) whereIndexes.get(counter)).intValue()]));
					}
					for (int counter = 0; counter < whereConditionSize; counter++) {
						if (((String) whereConditions.get(counter)).equalsIgnoreCase("and")) {
							flag = ((flags.get(counter))) && ((flags.get(counter + 1)));
						} else if (((String) whereConditions.get(counter)).equalsIgnoreCase("or")) {
							flag = ((flags.get(counter))) || ((flags.get(counter + 1)));
						}
					} // move to method
				} else {
					flag = evaluateWhereCondition(whereClause.get(0).getOperator(), whereClause.get(0).getValue(),
							lineData[(whereIndexes.get(0))]);
				}
				if (flag) {
					for (Integer i : indexes) {
						rowData.add(lineData[i].trim());
					}
					rowSet.put(rowCount, rowData);
					rowCount++;
				}
			}
		} catch (IOException e) {
			e.getMessage();
		}
		return rowSet;
	}

	// *******METHOD TO READ DATA FOR ORDER BY CLAUSE *****////////// method that works without condition of being into select list
	/* Map<Integer, ArrayList<String>> readData(String[] selectedColumns, String[] headers, String orderByColumn) {
	
		Map<Integer, ArrayList<String>> rowSet = readData(headers, headers);		
		int sortIndex = getIndex(orderByColumn, headers);

		// **** maps Sorting Logic Starts here it Uses Map Entry
		List<Map.Entry<Integer, ArrayList<String>>> entries = new ArrayList<Map.Entry<Integer, ArrayList<String>>>(
				rowSet.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<Integer, ArrayList<String>>>() {
			@Override
			public int compare(Entry<Integer, ArrayList<String>> firstRow,
					Entry<Integer, ArrayList<String>> secondRow) {
				try {
					Double value1 = Double.parseDouble(firstRow.getValue().get(sortIndex));
					Double value2 = Double.parseDouble(secondRow.getValue().get(sortIndex));
					return value1.compareTo(value2);
				} catch (Exception e) {
					return firstRow.getValue().get(sortIndex).compareTo(secondRow.getValue().get(sortIndex));
				}
			}
		});
		Map<Integer, ArrayList<String>> sortedMap = new LinkedHashMap<Integer, ArrayList<String>>();
		for (Map.Entry<Integer, ArrayList<String>> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		
		ArrayList<Integer> selectIndex = getIndexes(selectedColumns, headers);
		rowSet = new LinkedHashMap<>();;
		for (Map.Entry<Integer, ArrayList<String>> m : sortedMap.entrySet()) {
			ArrayList<String> row = new ArrayList<>();
			ArrayList<String> data = m.getValue();
			for(Integer index:selectIndex)
			{
				row.add(data.get(index));
			}
			rowSet.put(m.getKey(), row);
		}
		return rowSet;
	}
*/
	// *******METHOD TO READ DATA FOR ORDER BY CLAUSE *****//////////
	public Map<Integer, ArrayList<String>> readData(String[] selectedColumns, String[] headers, int orderByColumn,ArrayList<Criteria> whereClause, ArrayList<String> logicalConditions) {
		Map<Integer, ArrayList<String>> rowSet = null;
		
		if(whereClause == null && logicalConditions == null )
			rowSet = readData(selectedColumns, headers);
		else
			rowSet = readData(selectedColumns, headers,whereClause,logicalConditions);
		
		
		// **** maps Sorting Logic Starts here it Uses Map Entry
		List<Map.Entry<Integer, ArrayList<String>>> entries = new ArrayList<Map.Entry<Integer, ArrayList<String>>>(
				rowSet.entrySet());
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
	
	// ************************METHOD TO EVALUATE WHERE
	// CLAUSES********************//
	private boolean evaluateWhereCondition(String operator, String ClauseValue, String lineData) {
		boolean flag = false;
		switch (operator) {
		case ">":
			value = Double.parseDouble(ClauseValue);
			lineValue = Double.parseDouble(lineData);
			if (lineValue > value) {
				flag = true;
			}
			break;

		case "<":
			value = Double.parseDouble(ClauseValue);
			lineValue = Double.parseDouble(lineData);
			if (lineValue < value) {
				flag = true;
			}
			break;

		case "=":
			String stringValue = (ClauseValue);
			String stringLineValue = (lineData);
			if (stringLineValue.equalsIgnoreCase((stringValue))) {
				flag = true;
			}
			break;

		case ">=":
			value = Double.parseDouble(ClauseValue);
			lineValue = Double.parseDouble(lineData);
			if (lineValue >= value) {
				flag = true;
			}
			break;

		case "<=":
			value = Double.parseDouble(ClauseValue);
			lineValue = Double.parseDouble(lineData);
			if (lineValue <= value) {
				flag = true;
			}
			break;

		case "!=":
			String newValue = (ClauseValue);
			String newLineValue = (lineData);
			if (!(newLineValue.equalsIgnoreCase((newValue)))) {
				flag = true;
			}
			break;
		}
		// End of Switch Case
		return flag;
	}

}
