package com.stackroute.queryengine;

public class AggregateFunctions {

	String functionName;
	String columName;
	String value;
	
	public String getFunctionName() {
		return functionName;
	}
	public String getColumName() {
		return columName;
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
	
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "AggregateFunctions [functionName=" + functionName + ", columName=" + columName  + ", value=" + value + "]";
	}
}
