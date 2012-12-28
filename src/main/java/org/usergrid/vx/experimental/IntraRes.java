package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class IntraRes implements Serializable{
	
	private static final long serialVersionUID = 6994236506758407046L;
	private Object exception;
	private Integer exceptionId;
	private SortedMap<Integer,Object> opsRes;
	
	public IntraRes(){
		opsRes = new TreeMap<Integer,Object>();
	}
	public SortedMap<Integer, Object> getOpsRes() {
		return opsRes;
	}
	public void setOpsRes(SortedMap<Integer, Object> opsRes) {
		this.opsRes = opsRes;
	}
	public Object getException() {
		return exception;
	}
	public void setException(Object exception) {
		this.exception = exception;
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if (exception!=null)
			sb.append(exception+"\t");
		sb.append(opsRes.toString());
		return sb.toString();
	}
  public Integer getExceptionId() {
    return exceptionId;
  }
  public void setExceptionId(Integer exceptionId) {
    this.exceptionId = exceptionId;
  }
	
  public void setExceptionAndId(Object exception, int id){
    this.setException(exception);
    this.setExceptionId(id);
  }
}
