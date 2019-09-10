package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.internal.util.scheduler.*;

import java.sql.*;

public class DeRegister implements DriverAction {

  @Override
  public void deregister() {
    SchedulerServiceProviderHolder.getSchedulerProvider().close();
  }
}
