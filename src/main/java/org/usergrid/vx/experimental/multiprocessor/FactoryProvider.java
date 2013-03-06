package org.usergrid.vx.experimental.multiprocessor;

import org.usergrid.vx.experimental.multiprocessor.groovy.GroovyClMultiProcessorFactory;
import org.usergrid.vx.experimental.multiprocessor.groovy.GroovyMultiProcessorFactory;

public class FactoryProvider {

  public MultiProcessorFactory getFilterFactory(String spec) {
    switch (spec) {
      case "groovyscript":
        return new GroovyMultiProcessorFactory();
      case "groovyclassloader":
        return new GroovyClMultiProcessorFactory();
      default:
        throw new IllegalArgumentException(spec + " is not yet supported for filters");
    }
  }

}
