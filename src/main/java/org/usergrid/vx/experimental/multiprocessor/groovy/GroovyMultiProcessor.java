package org.usergrid.vx.experimental.multiprocessor.groovy;

import groovy.lang.Closure;

import java.util.List;
import java.util.Map;

import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;

public class GroovyMultiProcessor implements MultiProcessor {

  private Closure closure;

  public GroovyMultiProcessor(Closure closure) {
    this.closure = closure;
  }

  @Override
  public List<Map> multiProcess(Map<Integer, Object> results, Map params) {
    return (List<Map>) closure.call(results, params);
  }

}
