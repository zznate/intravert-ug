package org.usergrid.vx.experimental.multiprocessor;

import org.usergrid.vx.experimental.multiprocessor.groovy.GroovyMultiProcessorFactory;

public class FactoryProvider {

  public MultiProcessorFactory getFilterFactory(String spec) {
    switch (spec) {
      case "groovyclassloader":
        return new GroovyMultiProcessorFactory();
      default:
        throw new IllegalArgumentException(spec + " is not yet supported for filters");
    }
  }

}
