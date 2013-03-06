package org.usergrid.vx.experimental.processor;

public interface ProcessorFactory {
  public Processor createProcessor(String script);
}
