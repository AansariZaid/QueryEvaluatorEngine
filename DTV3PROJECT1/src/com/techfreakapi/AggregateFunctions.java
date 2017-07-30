package com.techfreakapi;

public class AggregateFunctions {

	String functionName;
	String columName;
	int Position;
	String value;
	
	
	
	public String getFunctionName() {
		return functionName;
	}
	public String getColumName() {
		return columName;
	}
	public int getPosition() {
		return Position;
	}
	public String getValue() {
		return value;
	}
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}
	public void setColumName(String columName) {
		this.columName = columName;
	}
	public void setPosition(int position) {
		Position = position;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
}
