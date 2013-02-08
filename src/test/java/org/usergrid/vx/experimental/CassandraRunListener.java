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

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

/**
 * @author zznate
 */
public class CassandraRunListener extends RunListener {

  @Override
  public void testRunStarted(Description description) throws Exception {

  }

  @Override
  public void testRunFinished(Result result) throws Exception {

  }

  //---------------- not used yet

  @Override
  public void testStarted(Description description) throws Exception {
    super.testStarted(description);
  }

  @Override
  public void testFinished(Description description) throws Exception {
    super.testFinished(description);
  }

  @Override
  public void testFailure(Failure failure) throws Exception {
    super.testFailure(failure);
  }

  @Override
  public void testAssumptionFailure(Failure failure) {
    super.testAssumptionFailure(failure);
  }

  @Override
  public void testIgnored(Description description) throws Exception {
    super.testIgnored(description);
  }
}
