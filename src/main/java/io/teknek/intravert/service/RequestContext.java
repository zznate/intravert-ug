package io.teknek.intravert.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RequestContext {

  private static final AtomicLong SESSION_ID = new AtomicLong(0);
  private static Map<Long,Session> SAVED = new HashMap<Long,Session>();
  
  private Session session;
  
  public RequestContext(){
    
  }
  
  public Session recoverSession(Long l){
    session = SAVED.get(l);
    return session;
  }
  
  public Long saveSession(){
    Long id = SESSION_ID.getAndIncrement();
    SAVED.put(id, getSession());
    return id;
  }
  
  public Session getSession(){
    if (session == null){
      session = new Session();
    }
    return session;
  }
  
}
