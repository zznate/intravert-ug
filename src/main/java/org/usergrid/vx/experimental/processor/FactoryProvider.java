package org.usergrid.vx.experimental.processor;

import org.usergrid.vx.experimental.processor.groovy.GroovyProcessorFactory;

public class FactoryProvider {

  public ProcessorFactory getFilterFactory(String spec) {
    switch (spec) {
      case "groovy":
        return new GroovyProcessorFactory();
      default:
        throw new IllegalArgumentException(spec + " is not yet supported for filters");
    }
  }

}
