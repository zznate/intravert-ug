package org.usergrid.vx.experimental;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Script;
import org.mozilla.javascript.Scriptable;

/**
 * @author John Sanda
 */
public class RhinoTest {

    @Test
    public void executeScript() throws Exception {
        String source =
            "var x = 1;\n" +
            "function over21(age) { if (age > 21) return true; else return false; }\n";
        Context context = Context.enter();
        Scriptable scope = context.initStandardObjects();
        Script compiledScript = context.compileString("function(row) { return row; }", "test", 1, null);
        Object result = context.evaluateString(scope, source, "test", 1, null);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "col1");
        map.put("value", "22");
        Scriptable jsObject = context.newObject(scope);
        jsObject.put("name", jsObject, "col1");
        jsObject.put("value", jsObject, "22");
        Function function = context.compileFunction(scope, "function filter(row) { return row; };", "test", 1, null);
        Scriptable jsRow = (Scriptable) function.call(context, scope, function, new Object[] {jsObject});
        Context.exit();
    }

}
