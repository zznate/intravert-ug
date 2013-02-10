package org.usergrid.vx.experimental.filter.groovy;

import org.usergrid.vx.experimental.Filter;

import groovy.lang.Closure;
import groovy.lang.GroovyShell;

/**
 * @author John Sanda
 */
public class GroovyFilterFactory {

    public Filter createFilter(String script) {
        GroovyShell shell = new GroovyShell();
        Object result = shell.evaluate(script);
        if (result instanceof Filter) {
            return (Filter) result;
        } else if (result instanceof Closure) {
            return new GroovyFilter((Closure) result);
        } else {
            throw new RuntimeException("Cannot create filter. Script must return either a closure or an instace " +
                "of " + Filter.class.getName());
        }
    }

}
