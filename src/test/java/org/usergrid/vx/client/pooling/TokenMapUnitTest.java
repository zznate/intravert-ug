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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author zznate
 */
public class TokenMapUnitTest {

  Map<String,Collection<String>> allTokens;

  @Before
  public void setupLocal() {
    allTokens = new HashMap();
    allTokens.put("host1", Arrays.asList("0","113427455640312821154458202477256070485"));
    allTokens.put("host2", Arrays.asList("56713727820156407428984779325531226112","0"));
    allTokens.put("host3", Arrays.asList("113427455640312821154458202477256070485","56713727820156407428984779325531226112"));
  }

  @Test
  public void buildSimpleThreeRFTwoCluster() {
    TokenMap tokenMap = TokenMap.build(TokenType.RP, allTokens);
    assertEquals(2, tokenMap.getReplicas(ByteBufferUtil.bytes("2835686391007827")).size());
  }
}
