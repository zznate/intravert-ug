package org.usergrid.vx.experimental.multiprocessor.groovy;

import groovy.lang.GroovyClassLoader;

import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;
import org.usergrid.vx.experimental.multiprocessor.MultiProcessorFactory;

public class GroovyClMultiProcessorFactory implements MultiProcessorFactory{

  @Override
  public MultiProcessor createMultiProcessor(String script) {
    GroovyClassLoader gc = new GroovyClassLoader();
    Class<?> c = gc.parseClass( script) ;
    MultiProcessor p = null;
    try {
      p = (MultiProcessor) c.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException (e);
    }
    return p;
  }

}

