/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.vertx.java.core.json.JsonObject;

public class IntraRes implements Serializable {

  private static final long serialVersionUID = 6994236506758407046L;

  private Object exception;

  private Integer exceptionId;

  private SortedMap<Object, Object> opsRes;

  public IntraRes() {
    opsRes = new TreeMap<Object, Object>();
  }

  public SortedMap<Object, Object> getOpsRes() {
    return opsRes;
  }

  public void setOpsRes(SortedMap<Object, Object> opsRes) {
    this.opsRes = opsRes;
  }

  public Object getException() {
    return exception;
  }

  public void setException(Object exception) {
    this.exception = exception;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (exception != null)
      sb.append(exception + "\t");
    sb.append(opsRes.toString());
    return sb.toString();
  }

  public Integer getExceptionId() {
    return exceptionId;
  }

  public void setExceptionId(Integer exceptionId) {
    this.exceptionId = exceptionId;
  }

  public void setExceptionAndId(Object exception, int id) {
    this.setException(exception);
    this.setExceptionId(id);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.putObject("exception", null);
    json.putString("exceptionId", String.valueOf(exceptionId));
    json.putElement("opsRes", new JsonObject((Map) opsRes));
    return json;
  }
}
