package org.usergrid.vx.experimental.filter;

public interface FilterFactory {
    Filter createFilter(String script);
}
