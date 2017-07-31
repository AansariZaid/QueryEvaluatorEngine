package com.stackroute.queryengine.processors;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.stackroute.queryengine.CSVFileReader;
import com.stackroute.queryengine.Criteria;
import com.stackroute.queryengine.QueryParameter;

public class SimpleProcessor implements Processor {

	private CSVFileReader csvFileReader = new CSVFileReader();
	private BufferedReader bufferedReader = null;
	private String line = null;
	private String tableName = null;
	private String[] lineData = null;
	private String[] headers = null;
	private String[] selectedColumnNames = null;
	private Map<Integer, ArrayList<String>> dataSet = null;
	private ArrayList<String> rowData = null;
	private ArrayList<Integer> indexes = null;
	private ArrayList<Integer> whereIndexes = null;
	private boolean flag = false;

	@Override
	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter) {

		tableName = queryParameter.getTableName();
		headers = csvFileReader.fetchHeader(tableName);
		selectedColumnNames = queryParameter.getSelectColumnNames();

		// replace select columns with all columns in header if * is present in query
		if (selectedColumnNames[0].equals("*")) {
			selectedColumnNames = headers;
		}
		// get indexes of columns
		indexes = csvFileReader.getIndexes(selectedColumnNames, headers);
		ArrayList<String> whereConditions = queryParameter.getlogicalConditions();
		ArrayList<Criteria> whereClause = queryParameter.getWhereClause();
		//RETRIVE WHERE COLUMN INDEXES ONLY IF WHERE CLAUSE EXISTS
		if (whereClause != null)
			whereIndexes = csvFileReader.getWhereClauseIndexes(whereClause, headers);

		dataSet = new LinkedHashMap<>();
		int rowCount = 0;
		ArrayList<Boolean> flags = null;
		try {
			bufferedReader = csvFileReader.bufferedReader;
			while ((this.line = bufferedReader.readLine()) != null) {
				lineData = line.split(",");
				rowData = new ArrayList<>();
				flag = false;

				// perform where Clause related operations only if where Clause Exists.
				if (whereClause != null) {
					int whereConditionSize = queryParameter.getlogicalConditions().size();
					flags = new ArrayList<>();
					if (whereConditionSize > 0) {
						for (int counter = 0; counter <= whereConditionSize; counter++) {
							flags.add(csvFileReader.evaluateWhereCondition(
									((Criteria) whereClause.get(counter)).getOperator(),
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
						flag = csvFileReader.evaluateWhereCondition(whereClause.get(0).getOperator(),
								whereClause.get(0).getValue(), lineData[(whereIndexes.get(0))]);
					}
				} else {
					flag = true;
				}
				if (flag) {
					// adding row data from splitted string element
					for (Integer i : indexes) {
						rowData.add(lineData[i].trim());
					}
					// putting each row into dataSet
					dataSet.put(rowCount, rowData);
					rowCount++;
				}
			}
		} catch (IOException e) {
			e.getMessage();
		}
		return dataSet;
	}

}
