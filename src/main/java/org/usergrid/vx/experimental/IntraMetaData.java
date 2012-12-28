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
