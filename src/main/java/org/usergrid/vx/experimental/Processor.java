package org.usergrid.vx.experimental;

import java.util.List;
import java.util.Map;

public interface Processor {
  public List<Map> process(List<Map> input);
}
