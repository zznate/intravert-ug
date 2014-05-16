package io.teknek.intravert.client;

import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.map.ObjectMapper;

public class Client {

  private ObjectMapper MAPPER = new ObjectMapper();
  private DefaultHttpClient httpClient = new DefaultHttpClient();
  
  @Deprecated
  public String postAsString(String url, String message)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {

    HttpPost postRequest = new HttpPost(url);

    StringEntity input = new StringEntity(message);
    input.setContentType("application/json");
    postRequest.setEntity(input);

    HttpResponse response = httpClient.execute(postRequest);

    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
              + response.getStatusLine().getStatusCode());
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(
            (response.getEntity().getContent())));

    String output;
    StringBuffer totalOutput = new StringBuffer();
    System.out.println("Output from Server .... \n");
    while ((output = br.readLine()) != null) {
      System.out.println(output);
      totalOutput.append(output);
    }
    return totalOutput.toString();
  }
  
  public Response post(String url, Request request)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {
    HttpPost postRequest = new HttpPost(url);
    ByteArrayEntity input = new ByteArrayEntity(MAPPER.writeValueAsBytes(request));
    input.setContentType("application/json");
    postRequest.setEntity(input);
    HttpResponse response = httpClient.execute(postRequest);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
              + response.getStatusLine().getStatusCode());
    }
    Response r = MAPPER.readValue(response.getEntity().getContent(), Response.class);
    response.getEntity().getContent().close();
    return r;
  }
  
}
