package org.usergrid.vx.experimental.filter.groovy;

import org.usergrid.vx.experimental.filter.Filter;
import org.usergrid.vx.experimental.filter.FilterFactory;

import groovy.lang.GroovyClassLoader;

public class GroovyCLFilterFactory implements FilterFactory {

  @Override
  public Filter createFilter(String script) {
    GroovyClassLoader gc = new GroovyClassLoader();
    Class<?> c = gc.parseClass( script) ;
    Filter p = null;
    try {
      p = (Filter) c.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException (e);
    }
    return p;
  }
}

