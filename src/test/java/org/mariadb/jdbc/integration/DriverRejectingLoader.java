// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import java.net.URL;
import java.net.URLClassLoader;
import javax.sql.DataSource;
import org.mariadb.jdbc.NonRegisteringDriver;

/**
 * Reloads the connector, failing the test if anything asks it for org.mariadb.jdbc.Driver.
 *
 * <p>Parent is the class loader of {@link DataSource} : it provides javax.sql, so a datasource
 * built here stays castable, but not org.mariadb.jdbc, so every connector class is reloaded.
 */
final class DriverRejectingLoader extends URLClassLoader {

  DriverRejectingLoader() {
    super(new URL[] {connectorLocation()}, DataSource.class.getClassLoader());
  }

  private static URL connectorLocation() {
    return NonRegisteringDriver.class.getProtectionDomain().getCodeSource().getLocation();
  }

  /**
   * Instantiate a datasource inside this loader.
   *
   * @param type datasource class
   * @param url connection string
   * @return the isolated datasource
   * @throws ReflectiveOperationException if instantiation fails
   */
  DataSource dataSource(Class<? extends DataSource> type, String url)
      throws ReflectiveOperationException {
    return (DataSource) loadClass(type.getName()).getConstructor(String.class).newInstance(url);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if ("org.mariadb.jdbc.Driver".equals(name)) {
      throw new AssertionError(name + " registers itself in DriverManager, pinning this loader");
    }
    return super.findClass(name);
  }
}
