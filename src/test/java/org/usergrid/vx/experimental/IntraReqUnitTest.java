package org.usergrid.vx.experimental;

import static junit.framework.Assert.assertEquals;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class IntraReqUnitTest {

    @Test
    public void mapRequestToJson() {
        IntraOp setKeyspace = Operations.setKeyspaceOp("compks");
        IntraOp createKeyspace = Operations.createKsOp("compks", 1);

        IntraReq req = new IntraReq();
        req.add(setKeyspace);
        req.add(createKeyspace);

        JsonObject actual = req.toJson();

        JsonObject expected = new JsonObject();
        expected.putArray("e", new JsonArray(new Object[] {setKeyspace, createKeyspace}));

        assertEquals("Failed to map " + IntraReq.class + "to JsonObject", expected, actual);
    }

    @Test
    public void mapJsonToRequest() throws Exception {
        IntraOp setKeyspace = Operations.setKeyspaceOp("compks");
        IntraOp createKeyspace = Operations.createKsOp("compks", 1);

        ObjectMapper mapper = new ObjectMapper();
        JsonObject createKeyspaceJson = new JsonObject(mapper.writeValueAsString(createKeyspace));
        JsonObject setKeyspaceJson = new JsonObject(mapper.writeValueAsString(setKeyspace));

        JsonObject json = new JsonObject();
        json.putArray("e", new JsonArray(new Object[] {createKeyspaceJson, setKeyspaceJson}));

        IntraReq expected = new IntraReq();
        expected.add(createKeyspace);
        expected.add(setKeyspace);

        IntraReq actual = IntraReq.fromJson(json);

        assertRequestEquals("Failed to map JsonObject to " + IntraReq.class, expected, actual);
    }

    private void assertRequestEquals(String msg, IntraReq expected, IntraReq actual) {
        assertEquals(msg + " - The number of operations is wrong.", expected.getE().size(), actual.getE().size());
        for (int i = 0; i < expected.getE().size(); ++i) {
            IntraOp expectedOp = expected.getE().get(i);
            IntraOp actualOp = actual.getE().get(i);

            assertEquals(msg + " - The operation type is wrong.", expectedOp.getType(), actualOp.getType());
            assertEquals(msg + " - The operation body is wrong.", expectedOp.getOp(), actualOp.getOp());
        }
    }

}
