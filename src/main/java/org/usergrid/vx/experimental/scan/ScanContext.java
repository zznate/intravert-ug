package org.usergrid.vx.experimental.scan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ScanContext {
	public String ks;// cant change constructor?
	public String cf;// cant change constructor?
	public Object startRow;// cant change constructor?
	public Object endRow;// cant change constructor?
	public Object startCol;
	public Object endCol;
	public ScanFilter filter;
	public List<Map> results;

	public ScanContext() {
		results = new ArrayList<Map>();
	}

	public void collect(Map m) {
		results.add(m);
	}

}
