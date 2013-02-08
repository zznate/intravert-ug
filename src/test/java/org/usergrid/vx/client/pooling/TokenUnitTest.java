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
package org.usergrid.vx.client.pooling;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author zznate
 */
public class TokenUnitTest {

  @Test
  public void tokenEnumLookup() {
    Token t = TokenType.RP.fromString("0");
    Token greaterThan = TokenType.RP.fromString("65910644309090703860934805640084041852");
    Token same = TokenType.RP.fromString("0");
    assertEquals(1, greaterThan.compareTo(t));
    assertEquals(-1, t.compareTo(greaterThan));
    assertEquals(same, t);
  }
}
