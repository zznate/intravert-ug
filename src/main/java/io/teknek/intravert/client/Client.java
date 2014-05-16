package io.teknek.intravert.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class Client {

  public static String postToURL(String url, String message, DefaultHttpClient httpClient)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {

    System.out.println("WTF");
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
}
