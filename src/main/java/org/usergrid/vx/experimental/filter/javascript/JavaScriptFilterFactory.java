package org.usergrid.vx.experimental.filter.javascript;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;
import org.usergrid.vx.experimental.Filter;

/**
 * @author John Sanda
 */
public class JavaScriptFilterFactory {

    public Filter createFilter(String script) {
        try {
            Context context = Context.enter();
            Scriptable scope = context.initStandardObjects();
            Function function = context.compileFunction(scope, script, "filter", 1, null);

            return new JavaScriptFilter(context, scope, function);
        } finally {
            Context.exit();
        }
    }

}
