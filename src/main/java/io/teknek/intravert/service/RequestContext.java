package io.teknek.intravert.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class RequestContext {

  private static final AtomicLong SESSION_ID = new AtomicLong(0);
  private static LoadingCache<Long,Session> CACHE; 
  private Session session;
  
  static {
    CacheLoader <Long,Session> loader = new CacheLoader<Long,Session>(){
      @Override
      public Session load(Long id) throws Exception {
        return new Session();
      }
    };
    CACHE = CacheBuilder.newBuilder()
            .maximumSize(10000)
            //.expireAfterAccess(60, TimeUnit.SECONDS)
            .build(loader);
  }
  
  public RequestContext(){
    
  }
  
  public Session recoverSession(Long l){
    try {
      session = CACHE.get(l);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    return session;
  }
  
  public Long saveSession(){
    Long id = SESSION_ID.getAndIncrement();
    CACHE.put(id, getSession());
    return id;
  }
  
  public Session getSession(){
    if (session == null){
      session = new Session();
    }
    return session;
  }
  
}
