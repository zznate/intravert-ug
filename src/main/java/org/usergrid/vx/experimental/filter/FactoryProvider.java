package org.usergrid.vx.experimental.filter;

import org.usergrid.vx.experimental.filter.groovy.GroovyFilterFactory;
import org.usergrid.vx.experimental.filter.javascript.JavaScriptFilterFactory;

public class FactoryProvider {

    public FilterFactory getFilterFactory(String spec) {
        switch (spec) {
            case "groovy": return new GroovyFilterFactory();
            case "javascript": return new JavaScriptFilterFactory();
            default: throw new IllegalArgumentException(spec + " is not yet supported for filters");
        }
    }

}
