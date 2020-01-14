package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

import java.sql.DriverAction;

public class DeRegister implements DriverAction {

  @Override
  public void deregister() {
    SchedulerServiceProviderHolder.close();
  }
}
