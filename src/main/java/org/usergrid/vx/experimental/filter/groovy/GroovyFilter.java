package org.usergrid.vx.experimental.filter.groovy;

import java.util.Map;

import org.usergrid.vx.experimental.Filter;

import groovy.lang.Closure;

class GroovyFilter implements Filter {

    private Closure closure;

    public GroovyFilter(Closure closure) {
        this.closure = closure;
    }

    @Override
    public Map filter(Map row) {
        return (Map) closure.call(row);
    }
}
