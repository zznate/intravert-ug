package org.usergrid.vx.experimental.processor.groovy;

import groovy.lang.GroovyClassLoader;

import org.usergrid.vx.experimental.processor.Processor;
import org.usergrid.vx.experimental.processor.ProcessorFactory;

public class GroovyClProcessorFactory implements ProcessorFactory {

  @Override
  public Processor createProcessor(String script) {
    GroovyClassLoader gc = new GroovyClassLoader();
    Class<?> c = gc.parseClass( script) ;
    Processor p = null;
    try {
      p = (Processor) c.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException (e);
    }
    return p;
  }
 
}