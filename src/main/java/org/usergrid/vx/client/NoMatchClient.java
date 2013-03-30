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
package org.usergrid.vx.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

public class NoMatchClient implements Handler<HttpClientResponse> {
  @SuppressWarnings("unused")
  private static Logger logger = LoggerFactory.getLogger(HelloClient.class);
  private static Vertx vertx;
  private HttpClient httpClient;

  public static void main(String[] args) throws Exception {
    NoMatchClient hc = new NoMatchClient();
    hc.post();
  }

  public NoMatchClient(){
    vertx = Vertx.newVertx();
    this.httpClient = vertx.createHttpClient().setHost("localhost").setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
  }

  public void post(){
    HttpClientRequest req = httpClient.request("POST", "/intravert/wrtwrtrwt", this);
    String value ="";
    req.putHeader("content-length", value.length());
    req.write(value);
    req.exceptionHandler(new Handler<Exception>(){
      public void handle(Exception arg0) {
        System.out.println("Something went wrong in client "+arg0);
      }
    });
    req.end();
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void handle(HttpClientResponse response) {
    response.endHandler(new SimpleHandler() {
      public void handle() {
        System.out.println("This is the end. My only friend, the end.");
      }
    });
    response.dataHandler( new Handler<Buffer>(){
      public void handle(Buffer buf) {
        String s = new String( buf.getBytes() );
        System.out.println("Data recieved: "+s);
      }
    });
  }

}
