// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Configuration;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.*;

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
    Driver driver = new com.singlestore.jdbc.Driver();
    assertEquals(0, driver.getPropertyInfo(null, null).length);
    assertEquals(0, driver.getPropertyInfo("jdbc:bla//", null).length);

    Properties properties = new Properties();
    properties.put("password", "myPwd");
    DriverPropertyInfo[] driverPropertyInfos =
        driver.getPropertyInfo("jdbc:singlestore://localhost/db?user=root", properties);
    for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
      if (!"$jacocoData".equals(driverPropertyInfo.name)) {
        assertNotNull(
            driverPropertyInfo.description, "no description for " + driverPropertyInfo.name);
      }
    }
  }

  @Test
  public void basicInfo() {
    Driver driver = new com.singlestore.jdbc.Driver();
    assertEquals(3, driver.getMajorVersion());
    assertTrue(driver.getMinorVersion() > -1);
    assertTrue(driver.jdbcCompliant());
    assertThrows(SQLFeatureNotSupportedException.class, () -> driver.getParentLogger());
  }
}
