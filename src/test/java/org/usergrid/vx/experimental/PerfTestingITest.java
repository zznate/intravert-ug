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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/* unless testing performance chances this should like be ignored
@Ignore
*/
@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class PerfTestingITest {

	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void aSanityTest() throws Exception {
		final int ops = 1;
		IntraClient2 ic = new IntraClient2("localhost", 8080);
		String keyspace = "keyspace";
		String columnFamily = "columnfamily";
		IntraReq req = new IntraReq();
		req.add(Operations.setOp("rowzz", "col1", "7").set(keyspace, "myks")
				.set(columnFamily, "mycf")); // 1
		IntraRes res = null;
		res = ic.sendBlocking(req);
		System.out.println(res);
	}
	
	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void blockingIc2Test() throws Exception {
		final int ops = 25000;
		long start = System.currentTimeMillis();
		IntraClient2 ic = new IntraClient2("localhost", 8080);
		String ks = "myks";
		String cf = "mycf";
		String keyspace = "keyspace";
		String columnFamily = "columnfamily";
		String rk="rowzz";
		String col="col1";
		String value="7";
		for (int i = 0; i < ops; ++i) {
			IntraReq req = new IntraReq();
			req.add(Operations.setOp(rk, col, value ).set(keyspace, ks)
					.set(columnFamily, cf)); // 1
			IntraRes res = null;
			res = ic.sendBlocking(req);
			if (i % 1000 == 0) {
				System.out.println(i);
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);
	}

	
	
}
