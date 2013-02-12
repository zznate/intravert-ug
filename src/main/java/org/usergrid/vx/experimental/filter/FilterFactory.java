package org.usergrid.vx.experimental.filter;

import org.usergrid.vx.experimental.Filter;

public interface FilterFactory {

    Filter createFilter(String script);

}
