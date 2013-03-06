package org.usergrid.vx.experimental.processor.groovy;

import org.usergrid.vx.experimental.filter.Filter;
import org.usergrid.vx.experimental.filter.FilterFactory;
import org.usergrid.vx.experimental.processor.Processor;
import org.usergrid.vx.experimental.processor.ProcessorFactory;

import groovy.lang.Closure;
import groovy.lang.GroovyShell;

public class GroovyProcessorFactory implements ProcessorFactory {

  public Processor createProcessor(String script) {
    GroovyShell shell = new GroovyShell();
    Object result = shell.evaluate(script);
    if (result instanceof Processor) {
      return (Processor) result;
    } else if (result instanceof Closure) {
      return new GroovyProcessor((Closure) result);
    } else {
      throw new RuntimeException(
              "Cannot create processor. Script must return either a closure or an instace " + "of "
                      + Processor.class.getName());
    }
  }

}