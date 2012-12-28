package org.usergrid.vx.experimental;

import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test as requiring that the defined Keyspace be present.
 *
 * @author zznate
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface RequiresKeyspace {
  String ksName();

  /**
   * The replication factor. Defaults to 1
   * @return
   */
  int replication() default 1;

  /**
   * The replication strategy. Defaults to SimpleStrategy.class
   * @return
   */
  Class<? extends AbstractReplicationStrategy> strategy() default SimpleStrategy.class;

  /**
   * String representation of the strategy options
   * @return
   */
  String strategyOptions() default "";

}
