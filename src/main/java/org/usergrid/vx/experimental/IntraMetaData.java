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

public class IntraMetaData {
  public String keyspace;
  public String columnfamily;
  public String type;
  
  public IntraMetaData(){
    
  }
  
  public IntraMetaData(String ks,String cf,String ty){
    keyspace=ks;
    columnfamily = cf;
    type= ty;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((columnfamily == null) ? 0 : columnfamily.hashCode());
    result = prime * result + ((keyspace == null) ? 0 : keyspace.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IntraMetaData other = (IntraMetaData) obj;
    if (columnfamily == null) {
      if (other.columnfamily != null)
        return false;
    } else if (!columnfamily.equals(other.columnfamily))
      return false;
    if (keyspace == null) {
      if (other.keyspace != null)
        return false;
    } else if (!keyspace.equals(other.keyspace))
      return false;
    if (type == null) {
      if (other.type != null)
        return false;
    } else if (!type.equals(other.type))
      return false;
    return true;
  }
  
  
}
