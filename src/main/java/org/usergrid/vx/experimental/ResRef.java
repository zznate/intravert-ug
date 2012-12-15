package org.usergrid.vx.experimental;

import java.io.Serializable;

public class ResRef implements Serializable{

	private static final long serialVersionUID = -9047941594776655202L;

	private int resId;
	private String wanted;
	public ResRef(){
		
	}
	public int getResId() {
		return resId;
	}
	public void setResId(int resId) {
		this.resId = resId;
	}
	public String getWanted() {
		return wanted;
	}
	public void setWanted(String wanted) {
		this.wanted = wanted;
	}
	
}
