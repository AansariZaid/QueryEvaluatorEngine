package com.stackroute.queryengine;

import java.awt.GradientPaint;
import java.util.ArrayList;
import java.util.Map;

import org.omg.CORBA.PRIVATE_MEMBER;

import com.stackroute.queryengine.processors.AggregateProcessor;
import com.stackroute.queryengine.processors.GroupByProcessor;
import com.stackroute.queryengine.processors.OrderByProcessor;
import com.stackroute.queryengine.processors.Processor;
import com.stackroute.queryengine.processors.SimpleProcessor;

public class QueryProcessor {

	Map<Integer, ArrayList<String>> dataSet;

	private Processor processor;

	public Map<Integer, ArrayList<String>> processQuery(QueryParameter queryParameter) {

		/*
		 * String tableName = queryParameter.getTableName(); String groupByColumn =
		 * queryParameter.getGroupByColumn(); String orderByColumn =
		 * queryParameter.getOrderByColumn(); int orderByIndexInMap =
		 * queryParameter.getOrderByIndexInMap(); String[] selectedColumnNames =
		 * queryParameter.getSelectColumnNames(); ArrayList<Criteria> whereClause =
		 * queryParameter.getWhereClause(); String[] headers =
		 * csvFileReader.fetchHeader(tableName); ArrayList<String> logicalConditions =
		 * queryParameter.getlogicalConditions();
		 */

		switch (queryParameter.getQueryType()) {

		case "SIMPLE_QUERY":
			processor = new SimpleProcessor();
			break;
		case "ORDER_BY_QUERY":
			processor = new OrderByProcessor();
			break;
		case "GROUP_BY_QUERY":
			processor = new GroupByProcessor();
			break;
		case "AGGREGATE_QUERY":
			processor = new AggregateProcessor();
			break;
		default:
			break;
		}

		dataSet = processor.executeQuery(queryParameter);
		return dataSet;
	}

	/*
	 * public Map<Integer, ArrayList<String>> processQuery(QueryParameter
	 * queryParameter, CSVFileReader csvFileReader) {
	 * 
	 * String tableName = queryParameter.getTableName(); String groupByColumn =
	 * queryParameter.getGroupByColumn(); String orderByColumn =
	 * queryParameter.getOrderByColumn(); int orderByIndexInMap =
	 * queryParameter.getOrderByIndexInMap(); String[] selectedColumnNames =
	 * queryParameter.getSelectColumnNames(); ArrayList<Criteria> whereClause =
	 * queryParameter.getWhereClause(); String[] headers =
	 * csvFileReader.fetchHeader(tableName); ArrayList<String> logicalConditions =
	 * queryParameter.getlogicalConditions();
	 * 
	 * if ((selectedColumnNames != null) && (tableName != null)) { // checking
	 * presence of table name and columns if ((groupByColumn == null) &&
	 * (whereClause == null) && (orderByColumn == null)) { // Go inside Only if
	 * group by and Order By and Where Clause is Not Present if
	 * (selectedColumnNames[0].trim().equals("*")) { // check for Presence of *
	 * dataSet = csvFileReader.readData(headers, headers); } else { // Call if * is
	 * Not there in query dataSet = csvFileReader.readData(selectedColumnNames,
	 * headers); } // Go Inside if Where Clause is there } else if ((whereClause !=
	 * null) && (groupByColumn == null) && (orderByColumn == null)) { if
	 * (selectedColumnNames[0].trim().equals("*")) { dataSet =
	 * csvFileReader.readData(headers, headers, whereClause, logicalConditions); }
	 * else { dataSet = csvFileReader.readData(selectedColumnNames, headers,
	 * whereClause, logicalConditions); } // move in for only group by } else if
	 * (groupByColumn != null && whereClause == null && orderByColumn == null) { if
	 * (selectedColumnNames[0].trim().equals("*")) { // check for Presence of *
	 * 
	 * } else { // Call if * is Not there in query // select ColumName with Group by
	 * } // move in for where with Order by } else if (orderByColumn != null &&
	 * groupByColumn == null) { if (selectedColumnNames[0].trim().equals("*")) { //
	 * check for Presence of * // select * with order by dataSet =
	 * csvFileReader.readData(headers, headers,
	 * orderByIndexInMap,whereClause,logicalConditions); } else { // Call if * is
	 * Not there in query dataSet = csvFileReader.readData(selectedColumnNames,
	 * headers, orderByIndexInMap,whereClause,logicalConditions); } // move in where
	 * with group by } else if (whereClause != null && groupByColumn != null &&
	 * orderByColumn == null) {
	 * 
	 * if (selectedColumnNames[0].trim().equals("*")) { // check for Presence of *
	 * // select * from where with group by } else { // Call if * is Not there in
	 * query // select ColumName from where with Group by } // move in where with
	 * orderby } else if (whereClause != null && orderByColumn != null &&
	 * groupByColumn == null) {
	 * 
	 * if (selectedColumnNames[0].trim().equals("*")) { // check for Presence of *
	 * // select * from where with orderBy } else { // Call if * is Not there in
	 * query // select ColumName from where with OrderBy } // move in for where with
	 * group by and order by } else if (whereClause != null && orderByColumn != null
	 * && groupByColumn == null) {
	 * 
	 * if (selectedColumnNames[0].trim().equals("*")) { // check for Presence of *
	 * // select * from where with group by and orderBy } else { // Call if * is Not
	 * there in query // select ColumName from where with group by and OrderBy } } }
	 * else { return null; }
	 * 
	 * return dataSet; }
	 */
	/*
	 * public Map<Integer, ArrayList<String>> ProcessQuery1(QueryParameter
	 * queryParameter, CSVFileReader csvFileReader) {
	 * 
	 * if (orderByColumn == null) // move in if order by is not there { if
	 * (groupByColumn == null) // move in if group by is not there { if (whereClause
	 * == null) // Move in if Where Clause is Also not there { if
	 * (selectedColumnNames[0].trim().equals("*")) { // check for Presence of * //
	 * example select * from tableName dataSet =
	 * csvFileReader.readData(csvFileReader.fetchHeader(queryParameter.getTableName(
	 * )), csvFileReader.fetchHeader(queryParameter.getTableName())); } else { //
	 * Call if * is Not there in query // example select columNames from tableName
	 * dataSet = csvFileReader.readData(selectedColumnNames,
	 * csvFileReader.fetchHeader(queryParameter.getTableName())); }
	 * 
	 * } // Outer Part if Where Clause is there if
	 * (selectedColumnNames[0].trim().equals("*")) {
	 * 
	 * dataSet =
	 * csvFileReader.readData(csvFileReader.fetchHeader(queryParameter.getTableName(
	 * )), csvFileReader.fetchHeader(queryParameter.getTableName()), whereClause,
	 * logicalConditions);
	 * 
	 * } else {
	 * 
	 * dataSet = csvFileReader.readData(selectedColumnNames,
	 * csvFileReader.fetchHeader(queryParameter.getTableName()), whereClause,
	 * logicalConditions); } } // Outer Part to be used if group By is There
	 * 
	 * if (whereClause == null) { // move in if group by is without where if
	 * (selectedColumnNames[0].trim().equals("*")) {
	 * 
	 * // call method with Group By Facility for *
	 * 
	 * } else {
	 * 
	 * // call Method for Group By Facility with Multiple Columns }
	 * 
	 * } // Outer Part if Group by is there with Where Clause if
	 * (selectedColumnNames[0].trim().equals("*")) {
	 * 
	 * // call method with Group By Facility for *
	 * 
	 * } else { // call Method for Group By Facility with Multiple Columns }
	 * 
	 * } else { // Move in if Order By is Also There // repeat from group by to Last
	 * 
	 * }
	 * 
	 * return dataSet; }
	 */
}
