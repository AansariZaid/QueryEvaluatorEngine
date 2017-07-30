package com.techfreakapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParameter {

	private String[] selectColumnNames = null;
	private String tableName = null;
	private ArrayList<Criteria> whereClause = null;
	private ArrayList<String> logicalConditions = null;
	private String sumFunction = null;
	private String groupByColumn = null; // custom class with agg function name
	private String countFunction = null;
	private String orderByColumn = null;
	
	private ArrayList<AggregateFunctions> aggregateFunctions = null;
	
	private Pattern pattern;
	private Matcher matcher;

	// METHOD TO OBTAIN GROUP BY INDEX IN MAP
	int getGroupByIndexInMap() {
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
	
	/* method to create Aggregate Function */
	private void createAggregateFunction(String aggregate)
	{
		AggregateFunctions function = new AggregateFunctions();
		
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

	public QueryParameter extractParameter(String query) {

		query = query.toLowerCase();

		String[] splitAtGroupBy = null;
		String[] splitAtWhere = null;

		// select * from tableName

		String[] splitAtFrom = query.split("from");
		pattern = Pattern.compile("select (.*?) from (.*)+?");
		matcher = pattern.matcher(query.trim());
		if (matcher.find()) {
			if (matcher.group(1).trim().contains("sum")) {
				pattern = Pattern.compile("(\\(([\\w\\*]+)\\))");
				matcher = pattern.matcher(matcher.group(1));
				if (matcher.find()) {
					sumFunction = matcher.group(2);
				}
			}
			// count(abcd) ---> abcd
			if (matcher.group(1).trim().contains("count")) {
				pattern = Pattern.compile("(\\(([\\w\\*]+)\\))");
				matcher = pattern.matcher(matcher.group(1));
				if (matcher.find()) {
					countFunction = matcher.group(2);
				}
			}
			selectColumnNames = matcher.group(1).split("[\\s,]+");
			String[] tableName = splitAtFrom[1].split("[\\s.]+");
			this.tableName = tableName[1];
		}
		String[] splitAtOrderBy = query.split("order by");

		if (splitAtOrderBy.length > 1) {
			orderByColumn = splitAtOrderBy[1].trim();
		}

		splitAtGroupBy = splitAtOrderBy[0].split("group by");

		if (splitAtGroupBy.length > 1) {
			groupByColumn = splitAtGroupBy[1].trim();
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

	public String getSumFunction() {
		return sumFunction;
	}

	public String getCountFunction() {
		return countFunction;
	}

	public String toString() {
		return

		"QueryParameter [selectColumnNames=" + Arrays.toString(selectColumnNames) + ", tableName=" + tableName
				+ ", whereClause=" + whereClause + ", logicalConditions=" + logicalConditions + ", groupByColumn="
				+ groupByColumn + ", orderByColumn=" + orderByColumn + ", sumFunction=" + sumFunction
				+ ", countFunction=" + countFunction + "]";
	}

	public ArrayList<String> getlogicalConditions() {
		return logicalConditions;
	}

	public String getOrderByColumn() {
		return orderByColumn;
	}
}