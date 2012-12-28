package org.usergrid.vx.experimental;

import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.Arrays;

/**
 * @author zznate
 */
public class CassandraRunner extends BlockJUnit4ClassRunner {

  private static final Logger logger = LoggerFactory.getLogger(CassandraRunner.class);

  public CassandraRunner(Class<?> klass) throws InitializationError {
    super(klass);
    logger.info("CassandraRunner constructed with class {}", klass.getName());
  }

  @Override
  protected void runChild(FrameworkMethod method, RunNotifier notifier) {
    logger.info("runChild invoked on method: " + method.getName());
    if ( method.getAnnotation(RequiresKeyspace.class) != null ) {
      maybeCreateKeyspace(method.getAnnotation(RequiresKeyspace.class));
    }
    if ( method.getAnnotation(RequiresColumnFamily.class) != null) {
      maybeCreateColumnFamily(method.getAnnotation(RequiresColumnFamily.class));
    }
    if (method.getAnnotation(DataLoader.class) != null) {
      logger.info("Loading dataset {}", method.getAnnotation(DataLoader.class).dataset());
    }

    super.runChild(method, notifier);
  }

  @Override
  public void run(RunNotifier notifier) {
    for (Annotation ann : getTestClass().getAnnotations() ) {
      if ( ann instanceof RequiresKeyspace ) {
        maybeCreateKeyspace((RequiresKeyspace)ann);
      } else if ( ann instanceof RequiresColumnFamily ) {
        maybeCreateColumnFamily((RequiresColumnFamily) ann);
      }
    }
    super.run(notifier);
  }

  private void maybeCreateKeyspace(RequiresKeyspace rk) {
    logger.info("RequiresKeyspace annotation has ksName: {}", rk.ksName());

  }

  private void maybeCreateColumnFamily(RequiresColumnFamily rcf) {
    logger.info("RequiresColumnFamily annotation has name: {} for ks: {}", rcf.cfName(), rcf.ksName());
  }
}
