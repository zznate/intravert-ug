package org.usergrid.vx.experimental.processor;

import org.usergrid.vx.experimental.processor.groovy.GroovyClProcessorFactory;
import org.usergrid.vx.experimental.processor.groovy.GroovyProcessorFactory;

public class FactoryProvider {

  public ProcessorFactory getFilterFactory(String spec) {
    switch (spec) {
      case "groovyscript":
        return new GroovyProcessorFactory();
      case "groovyclassloader":
        return new GroovyClProcessorFactory();
      default:
        throw new IllegalArgumentException(spec + " is not yet supported for filters");
    }
  }

}
