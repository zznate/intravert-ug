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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;
import org.usergrid.vx.client.IntraClient2.Transport;
import org.usergrid.vx.client.thrift.FramedConnWrapper;
import org.vertx.java.core.Vertx;

/* unless testing performance chances this should like be ignored */
@Ignore

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class PerfTestingITest {
	
	  Vertx x = Vertx.newVertx();

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
	
	
	
	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void blockingIc2SmileTest() throws Exception {
		final int ops = 25000;
		long start = System.currentTimeMillis();
		IntraClient2 ic = new IntraClient2("localhost", 8080);
		ic.setTransport(Transport.SMILE);
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
	
	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void blitz() throws Exception {
		
		List<Loader> l = new ArrayList<Loader>();
		for (int i=0; i< 100000;i+=10000){
			l.add( new Loader(i,i+9999) );
		}
		
		long start = System.currentTimeMillis();
		ThreadGroup tg = new ThreadGroup("loaders");
		for (int j=0; j< l.size(); j++){
			Thread t = new Thread(tg,(Runnable) l.get(j));
			t.start();
		}
		 
		boolean allDone = false;
		while (allDone == false){
			boolean anyRunning = false;
			for (int i =0;i<l.size();i++){
				if (! l.get(i).done ){
					anyRunning=true;
				}
			}
			if (anyRunning==false){
				allDone=true;
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("time take " + (end-start));
		Thread.sleep(5000);
	}
	
	class Loader implements Runnable{
		int start;
		int end;
		int i;
		boolean done;
		public Loader(int start, int end){
			this.start=start;
			this.end=end;
		}
		@Override
		public void run() {
			IntraClient2 ic = new IntraClient2("localhost", 8080);
			for (i=start;i<end;i++){
				IntraReq req = new IntraReq();
				req.add(Operations.setOp(i+"", i+"", "" ).set("keyspace", "myks")
						.set("columnfamily", "mycf")); // 1
				IntraRes res = null;
				try {
					res = ic.sendBlocking(req);
				} catch (Exception e) {
					//e.printStackTrace();
				}
				if (i % 1000 == 0) {
					System.out.println(i);
				}
			}
			done=true;
		}
		
	}
	@Test
	public void internalTest(){
		List<InternalLoader> l = new ArrayList<InternalLoader>();
		for (int i=0; i< 100000;i+=10000){
			l.add( new InternalLoader(i,i+9999) );
		}
		
		long start = System.currentTimeMillis();
		ThreadGroup tg = new ThreadGroup("loaders");
		for (int j=0; j< l.size(); j++){
			Thread t = new Thread(tg,(Runnable) l.get(j));
			t.start();
		}
		 
		boolean allDone = false;
		while (allDone == false){
			boolean anyRunning = false;
			for (int i =0;i<l.size();i++){
				if (! l.get(i).done ){
					anyRunning=true;
				}
			}
			if (anyRunning==false){
				allDone=true;
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("time take " + (end-start));
		
	}
	
	/*
	@Test
	public void map() throws  JsonMappingException, IOException{
		ObjectMapper m = new ObjectMapper();
		long start = System.currentTimeMillis();
		for (int i=0;i<100000;i++){
			IntraReq req = new IntraReq();
			req.add(Operations.setOp(i+"", i+"", "" ).set("keyspace", "myks")
					.set("columnfamily", "mycf")); // 1
			m.writeValue(new ByteArrayOutputStream(), req);
		}
		long end = System.currentTimeMillis();
		System.out.println("jackson"+ (end - start));
	}
	*/
	class InternalLoader implements Runnable{
		int start;
		int end;
		int i;
		boolean done;
		public InternalLoader(int start, int end){
			this.start=start;
			this.end=end;
		}
		@Override
		public void run() {
		  IntraClient2 ic2 = new IntraClient2("localhost",8080);
			for (i=start;i<end;i++){
				IntraReq req = new IntraReq();
				req.add(Operations.setOp(i+"", i+"", "" ).set("keyspace", "myks")
						.set("columnfamily", "mycf")); // 1
				IntraRes res = new IntraRes();
				try {
				  
			    res = ic2.sendBlocking(req); 
				} catch (Exception e) {
					System.err.println(e);
				}
				if (i % 1000 == 0) {
					System.out.println(i);
				}
			}
			done=true;
		}
		
	}
	
	class LoadMap implements Runnable{
		int start;
		int end;
		int i;
		boolean done;
		public LoadMap(int start, int end){
			this.start=start;
			this.end=end;
		}
		@Override
		public void run() {
			ObjectMapper m = new ObjectMapper();
			for (i=start;i<end;i++){
				IntraReq req = new IntraReq();
				req.add(Operations.setOp(i+"", i+"", "" ).set("keyspace", "myks")
						.set("columnfamily", "mycf")); // 1
				try {
					m.writeValue(new ByteArrayOutputStream(), req);
				} catch (JsonGenerationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JsonMappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			done=true;
		}
		
	}
	
	
	@Test
	public void parallenMap(){
		List<LoadMap> l = new ArrayList<LoadMap>();
		for (int i=0; i< 1000000;i+=100000){
			l.add( new LoadMap(i,i+99999) );
		}
		
		long start = System.currentTimeMillis();
		ThreadGroup tg = new ThreadGroup("loaders");
		for (int j=0; j< l.size(); j++){
			Thread t = new Thread(tg,(Runnable) l.get(j));
			t.start();
		}
		 
		boolean allDone = false;
		while (allDone == false){
			boolean anyRunning = false;
			for (int i =0;i<l.size();i++){
				if (! l.get(i).done ){
					anyRunning=true;
				}
			}
			if (anyRunning==false){
				allDone=true;
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("time take " + (end-start));
		
	}
	
}
