package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IntraReq implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6101558885453116210L;
	private List<IntraOp> e = new ArrayList<IntraOp>();
	public IntraReq(){
		
	}
	public IntraReq add(IntraOp o){
		e.add(o);
		return this;
	}
	public List<IntraOp> getE() {
		return e;
	}
	public void setE(List<IntraOp> e) {
		this.e = e;
	}	
	public String toString(){
		return e.toString();
	}
}