package com.techfreakapi;

public class Criteria {
	
	private String columnName;
	private String Value;
	private String Operator;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getValue() {
		return Value;
	}
	public void setValue(String value) {
		Value = value;
	}
	public String getOperator() {
		return Operator;
	}
	public void setOperator(String operator) {
		Operator = operator;
	}
	@Override
	public String toString() {
		return "Criteria [columnName=" + columnName + ", Value=" + Value + ", Operator=" + Operator + "]";
	}
	
	

}
