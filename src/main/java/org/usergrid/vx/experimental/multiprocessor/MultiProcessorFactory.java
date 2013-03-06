package org.usergrid.vx.experimental.multiprocessor;


public interface MultiProcessorFactory {
  public MultiProcessor createMultiProcessor(String script);
}