package com.stackroute.queryengine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParameter {

	private String[] selectColumnNames = null;
	private String tableName = null;
	private ArrayList<Criteria> whereClause = null;
	private ArrayList<String> logicalConditions = null;
	private String groupByColumn = null;
	private String orderByColumn = null;
	private String queryType = "SIMPLE_QUERY";

	private ArrayList<AggregateFunctions> aggregateFunctions = new ArrayList<>();

	private Pattern pattern;
	private Matcher matcher;

	// METHOD TO OBTAIN GROUP BY INDEX IN MAP
	public int getGroupByIndexInMap() {
		int index = 0;
		for (int i = 0; i < selectColumnNames.length; i++) {
			if (selectColumnNames[i].equalsIgnoreCase(groupByColumn))
				index = i;
		}
		return index;
	}

	// METHOD TO OBTAIN ORDER BY INDEX IN MAP
	int getOrderByIndexInMap() {
		int index = 0;
		for (int i = 0; i < selectColumnNames.length; i++) {
			if (selectColumnNames[i].equalsIgnoreCase(orderByColumn))
				index = i;
		}
		return index;
	}

	// sal > 1000
	private void createCriteria(String clauseString) {
		Criteria criteria = new Criteria();
		pattern = Pattern.compile("(.*) ([!=|>=|<=|>|<|=]+) (.*)");
		matcher = pattern.matcher(clauseString);
		if (matcher.find()) {
			criteria.setColumnName(matcher.group(1).trim());
			criteria.setOperator(matcher.group(2).trim());
			criteria.setValue(matcher.group(3).trim());
		}
		whereClause.add(criteria);
	}

	/* method to create Aggregate Function */
	private void createAggregateFunction(String aggregate, String functions) {
		queryType = "AGGREGATE_QUERY";
		AggregateFunctions someFunction = new AggregateFunctions();
		pattern = Pattern.compile("(\\(([\\w\\*]+)\\))");
		matcher = pattern.matcher(aggregate);
		if (matcher.find()) {
			someFunction.setColumName(matcher.group(2));
			someFunction.setFunctionName(functions);
		}
		aggregateFunctions.add(someFunction);
	}

	// *********METHOD TO EXTRACT ALL PRAMETERS FROM GIVEN QUERY***//

	public QueryParameter extractParameter(String query) {

		query = query.toLowerCase();

		String[] splitAtGroupBy = null;
		String[] splitAtWhere = null;

		String[] splitAtOrderBy = query.split("order by");

		splitAtGroupBy = splitAtOrderBy[0].split("group by");

		if (splitAtGroupBy.length > 1) {
			groupByColumn = splitAtGroupBy[1].trim();
			queryType = "GROUP_BY_QUERY";
		}

		if (splitAtOrderBy.length > 1) {
			orderByColumn = splitAtOrderBy[1].trim();
			queryType = "ORDER_BY_QUERY";
		}

		splitAtWhere = splitAtGroupBy[0].split("where");

		// sal > 1000 and name = abcd or col = something

		if (splitAtWhere.length > 1) {
			whereClause = new ArrayList<>();
			logicalConditions = new ArrayList<>();
			String whereString = splitAtWhere[1].trim();
			String[] logicalCond = whereString.split("\\s+");
			for (String s : logicalCond) {
				if (s.equals("and")) {
					logicalConditions.add("and");
				} else if (s.equals("or")) {
					logicalConditions.add("or");
				}
			}
			String[] whereClauseElement = whereString.split(" and | or ", 2); // extra spaces are handled by trim
			while (whereClauseElement.length != 1) {
				createCriteria(whereClauseElement[0].trim());
				whereClauseElement = whereClauseElement[1].split(" and | or ", 2);
			}
			createCriteria(whereClauseElement[0].trim());
		}
		// select * from tableName

		String[] splitAtFrom = query.split("from");
		// extracting column names

		String[] columns = splitAtFrom[0].split("\\s+|\\,");

		// extracting aggregate functions
		for (int i = 1; i < columns.length; i++) {
			if (columns[i].trim().contains("sum")) {
				createAggregateFunction(columns[i].trim(), "sum");
			}
			// count(abcd) ---> abcd
			if (columns[i].trim().trim().contains("count")) {
				createAggregateFunction(columns[i].trim(), "count");
			}
			if (columns[i].trim().trim().contains("min")) {
				createAggregateFunction(columns[i].trim(), "min");
			}
			if (columns[i].trim().trim().contains("max")) {
				createAggregateFunction(columns[i].trim(), "max");
			}
			if (columns[i].trim().trim().contains("avg")) {
				createAggregateFunction(columns[i].trim(), "avg");
			}
		}
		// extracting columns without aggregate Functions
		pattern = Pattern.compile("select (.*?) from (.*)+?");
		matcher = pattern.matcher(query.trim());
		if (matcher.find()) {
			selectColumnNames = matcher.group(1).split("[\\s,]+");
			String[] tableName = splitAtFrom[1].split("[\\s.]+");
			this.tableName = tableName[1];
		}

		return this;
	}

	public String[] getSelectColumnNames() {
		return selectColumnNames;
	}

	public String getTableName() {
		return tableName;
	}

	public ArrayList<Criteria> getWhereClause() {
		return whereClause;
	}

	public String getGroupByColumn() {
		return groupByColumn;
	}

	public ArrayList<String> getlogicalConditions() {
		return logicalConditions;
	}

	public String getOrderByColumn() {
		return orderByColumn;
	}

	@Override
	public String toString() {
		return "QueryParameter [selectColumnNames=" + Arrays.toString(selectColumnNames) + ", tableName=" + tableName
				+ ", whereClause=" + whereClause + ", logicalConditions=" + logicalConditions + ", groupByColumn="
				+ groupByColumn + ", orderByColumn=" + orderByColumn + ", queryType=" + queryType
				+ ", aggregateFunctions=" + aggregateFunctions + "]";
	}

	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}

}