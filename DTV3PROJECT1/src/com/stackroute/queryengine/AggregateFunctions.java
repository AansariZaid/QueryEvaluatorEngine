package com.stackroute.queryengine;

public class AggregateFunctions {

	String functionName;
	String columName;
	double value = 0.0;

	public String getFunctionName() {
		return functionName;
	}

	public String getColumName() {
		return columName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	public void setColumName(String columName) {
		this.columName = columName;
	}

	@Override
	public String toString() {
		return "AggregateFunctions [functionName=" + functionName + ", columName=" + columName + ", value=" + value
				+ "]";
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
}
