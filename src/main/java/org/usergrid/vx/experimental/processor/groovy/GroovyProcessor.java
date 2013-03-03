package org.usergrid.vx.experimental.processor.groovy;

import groovy.lang.Closure;

import java.util.List;
import java.util.Map;

import org.usergrid.vx.experimental.processor.Processor;

public class GroovyProcessor implements Processor {

  private Closure closure;
  
  public GroovyProcessor (Closure closure){
    this.closure=closure;
  }
  
  @Override
  public List<Map> process(List<Map> input) {
    return (List<Map>) closure.call(input);
  }

}
