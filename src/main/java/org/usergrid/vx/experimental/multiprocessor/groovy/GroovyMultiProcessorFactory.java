package org.usergrid.vx.experimental.multiprocessor.groovy;

import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;
import org.usergrid.vx.experimental.multiprocessor.MultiProcessorFactory;

import groovy.lang.Closure;
import groovy.lang.GroovyShell;

public class GroovyMultiProcessorFactory implements MultiProcessorFactory {

  @Override
  public MultiProcessor createMultiProcessor(String script) {
    GroovyShell shell = new GroovyShell();
    Object result = shell.evaluate(script);
    if (result instanceof MultiProcessor) {
      return (MultiProcessor) result;
    } else if (result instanceof Closure) {
      return new GroovyMultiProcessor((Closure) result);
    } else {
      throw new RuntimeException(
              "Cannot create processor. Script must return either a closure or an instace " + "of "
                      + MultiProcessor.class.getName());
    }
  }

}
