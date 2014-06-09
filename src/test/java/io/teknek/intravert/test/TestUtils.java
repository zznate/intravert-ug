package io.teknek.intravert.test;

import io.teknek.intravert.model.Response;
import junit.framework.Assert;

public class TestUtils {
  public static void assertResponseDidNotFail(Response response){
    Assert.assertNull(response.getExceptionMessage());
    Assert.assertNull(response.getExceptionId());
  }
}
