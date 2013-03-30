package org.usergrid.vx.experimental;

@Deprecated
public class NonAtomicReference {
	public Object something;
	public String toString(){
		return "non-atomic-reference: "+something;
	}
}
