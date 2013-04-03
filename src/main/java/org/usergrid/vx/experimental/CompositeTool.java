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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompositeTool {

  public static List<byte[]> bbArrayToByteArray(List<ByteBuffer> b) {
    List<byte[]> b1 = new ArrayList<byte[]>();
    for (ByteBuffer bb : b) {
      b1.add(ByteBufferUtil.getArray(bb));
    }
    return b1;
  }

  public static List<ByteBuffer> byteArrayToBBArray(List<byte[]> b) {
    List<ByteBuffer> b1 = new ArrayList<ByteBuffer>();
    for (byte[] bb : b) {
      b1.add(ByteBuffer.wrap(bb));
    }
    return b1;
  }

  public static byte[] makeComposite(List<byte[]> b, int[] sep) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < b.size(); i++) {
      bos.write((byte) ((b.get(i).length >> 8) & 0xFF));
      bos.write((byte) (b.get(i).length & 0xFF));
      for (int j = 0; j < b.get(i).length; j++) {
        bos.write(b.get(i)[j] & 0xFF);
      }
      bos.write((byte) (sep[i] & 0xFF));
    }
    return bos.toByteArray();
  }

  public static byte[] makeComposite(List<byte[]> b) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < b.size(); i++) {
      bos.write((byte) ((b.get(i).length >> 8) & 0xFF));
      bos.write((byte) (b.get(i).length & 0xFF));
      for (int j = 0; j < b.get(i).length; j++) {
        bos.write(b.get(i)[j] & 0xFF);
      }
      bos.write((byte) 0);
    }
    return bos.toByteArray();
  }

  public static List<byte[]> readComposite(byte[] column) {
    List<byte[]> result = new ArrayList<byte[]>();
    for (int i = 0; i < column.length; i++) {
      int length = (column[i++] & 0xFF) << 8;
      length = (column[i++] & 0xFF);
      byte[] data = new byte[length];
      for (int j = 0; j < length; j++) {
        data[j] = column[i++];
      }
      result.add(data);
    }
    return result;
  }
  
  public static void prettyPrintComposite(byte [] column, List<AbstractType> columnType){
    List<byte[]> parts = readComposite(column);
    for (int i =0;i<parts.size();i++){
      try {
      System.out.println( columnType.get(i).getString(ByteBuffer.wrap(parts.get(i))) );
      } catch (Exception ex){}
    }
  }
}