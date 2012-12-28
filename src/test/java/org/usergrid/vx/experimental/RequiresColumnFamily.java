package org.usergrid.vx.experimental;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test as requiring the defined columnFamily to be present.
 * Comparator and validator are UTF8Type by default.
 * @author zznate
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface RequiresColumnFamily {
  String ksName();
  String cfName();
  String comparator() default "UTF8Type";
  String defaultValidator() default "UTF8Type";

}
