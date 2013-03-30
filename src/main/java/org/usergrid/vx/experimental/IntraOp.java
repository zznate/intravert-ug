/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class IntraOp implements Serializable{

	public static final String COLUMN= "COLUMN";
	public static final String VALUE="VALUE";

	private static final long serialVersionUID = 4700563595579293663L;
	private Type type;
	private Map<String,Object> op;

  protected IntraOp() {}
  	public String toString(){
  		return this.type + " "+this.op;
  	}
	IntraOp(Type type){
    this.type = type;
		op = new TreeMap<String,Object>();
	}

	public IntraOp set(String key, Object value){
		op.put(key,value);
		return this;
	}

	public Map<String, Object> getOp() {
		return op;
	}

	public Type getType() {
		return type;
	}


  public enum Type {
    LISTCOLUMNFAMILY ,
    LISTKEYSPACES ,
    CONSISTENCY ,
    CREATEKEYSPACE ,
    CREATECOLUMNFAMILY ,
    FOREACH ,
    SLICEBYNAMES ,
    SLICE ,
    COUNTER ,
    GET ,
    SET ,
    AUTOTIMESTAMP ,
    SETCOLUMNFAMILY ,
    ASSUME ,
    CREATEPROCESSOR ,
    PROCESS ,
    DROPKEYSPACE ,
    CREATEFILTER ,
    FILTERMODE ,
    CQLQUERY ,
    CLEAR ,
    CREATEMULTIPROCESS ,
    MULTIPROCESS ,
    STATE ,
    BATCHSET , 
    CREATESERVICEPROCESS, 
    SERVICEPROCESS , 
    COMPONENTSELECT ,
		PREPARE ,
		EXECUTEPREPARED ,  
		CREATESCANFILTER,
    NOOP ,
    SETKEYSPACE
  };

}
