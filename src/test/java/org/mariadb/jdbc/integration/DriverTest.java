/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;
import org.mariadb.jdbc.Configuration;

public class DriverTest extends Common {

  @Test
  public void ensureDescriptionFilled() throws Exception {
    Properties descr = new Properties();
    try (InputStream inputStream =
        Common.class.getClassLoader().getResourceAsStream("driver.properties")) {
      descr.load(inputStream);
    }

    // check that description is present
    for (Field field : Configuration.Builder.class.getDeclaredFields()) {
      if (!field.getName().startsWith("_")) {
        if (descr.get(field.getName()) == null && !"$jacocoData".equals(field.getName()))
          throw new Exception(String.format("Missing %s description", field.getName()));
      }
    }

    // check that no description without option
    for (Map.Entry<Object, Object> entry : descr.entrySet()) {
      // NoSuchFieldException will be thrown if not present
      Configuration.Builder.class.getDeclaredField(entry.getKey().toString());
    }
  }

  @Test
  public void getPropertyInfo() throws SQLException {
    Driver driver = new org.mariadb.jdbc.Driver();
    assertEquals(0, driver.getPropertyInfo(null, null).length);
    assertEquals(0, driver.getPropertyInfo("jdbc:bla//", null).length);

    Properties properties = new Properties();
    properties.put("password", "myPwd");
    DriverPropertyInfo[] driverPropertyInfos =
        driver.getPropertyInfo("jdbc:mariadb://localhost/db?user=root", properties);
    for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
      if (!"$jacocoData".equals(driverPropertyInfo.name)) {
        assertNotNull(
            driverPropertyInfo.description, "no description for " + driverPropertyInfo.name);
      }
    }
  }

  @Test
  public void basicInfo() {
    Driver driver = new org.mariadb.jdbc.Driver();
    assertEquals(3, driver.getMajorVersion());
    assertTrue(driver.getMinorVersion() > -1);
    assertTrue(driver.jdbcCompliant());
    assertThrows(SQLFeatureNotSupportedException.class, () -> driver.getParentLogger());
  }
}
