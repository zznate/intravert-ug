package org.usergrid.vx.experimental;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

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

    @SuppressWarnings("unchecked")
    public JsonObject toJson() {
        return new JsonObject().putArray("e", new JsonArray((List) e));
    }

    public static IntraReq fromJson(JsonObject json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            IntraReq req = new IntraReq();
            JsonArray array = json.getArray("e");
            for (Object o : array) {
                IntraOp op = mapper.readValue(o.toString(), IntraOp.class);
                req.add(op);
            }
            return req;
        } catch (IOException e1) {
            throw new RuntimeException("Fail to convert JsonObject into IntraReq", e1);
        }
    }
	
}
