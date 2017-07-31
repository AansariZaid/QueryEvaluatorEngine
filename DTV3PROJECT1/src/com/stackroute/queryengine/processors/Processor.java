package com.stackroute.queryengine.processors;

import java.util.ArrayList;
import java.util.Map;

import com.stackroute.queryengine.CSVFileReader;
import com.stackroute.queryengine.QueryParameter;

public interface Processor {

	public Map<Integer, ArrayList<String>> executeQuery(QueryParameter queryParameter);
}
