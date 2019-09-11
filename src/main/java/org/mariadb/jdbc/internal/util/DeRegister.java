package org.mariadb.jdbc.internal.util;

import java.sql.DriverAction;
import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

public class DeRegister implements DriverAction {

  @Override
  public void deregister() {
    SchedulerServiceProviderHolder.close();
  }
}
