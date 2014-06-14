package io.teknek.intravert.daemon;

import io.teknek.intravert.client.Client;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import junit.framework.Assert;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.junit.Before;
import org.junit.Test;

public class JsonFileTest extends BaseIntravertTest {

  private File jtest;
  
  @Before
  public void before(){
    File resources = new File("src/test/resources");
    jtest = new File(resources, "jtest");
  }
  
  public void innerTest(String testname) throws JsonParseException, JsonMappingException, IOException{
    File testDir = new File(jtest, testname);
    if (!testDir.isDirectory()){
      throw new RuntimeException(testDir+" is not a directory");
    }
    File input = new File(testDir, "input.json");
    File output = new File(testDir, "output.json");
    Client c = new Client();
    ObjectMapper om  = new ObjectMapper();
    om.configure(Feature.INDENT_OUTPUT, true);
    Request r = om.readValue(input, Request.class);
    Response resp = c.post("http://localhost:7654", r);
    Assert.assertEquals(new String(Files.readAllBytes(output.toPath())).trim(), om.writeValueAsString(resp).trim());
  }
  
  @Test
  public void putGetTest() throws JsonParseException, JsonMappingException, IOException{
    innerTest("putget");
  }
}
