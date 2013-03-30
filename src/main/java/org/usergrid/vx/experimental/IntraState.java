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

import org.apache.cassandra.db.ConsistencyLevel;
import org.usergrid.vx.experimental.filter.Filter;
import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;
import org.usergrid.vx.experimental.processor.Processor;
import org.usergrid.vx.experimental.scan.ScanContext;
import org.usergrid.vx.experimental.scan.ScanFilter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/* class that holds properties for the request lifecycle */
@Deprecated
public class IntraState {
	
	public static Map<String,Filter> filters = new HashMap<String,Filter>();
	Filter currentFilter;
	
}
