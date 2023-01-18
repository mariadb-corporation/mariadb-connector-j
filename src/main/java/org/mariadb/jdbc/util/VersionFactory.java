// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class VersionFactory {
  private static volatile Version instance = null;

  // use getShape method to get object of type shape
  public static Version getInstance() {
    if (instance == null) {
      synchronized (VersionFactory.class) {
        if (instance == null) {
          String tmpVersion = "5.5.0";
          try (InputStream inputStream =
              Version.class.getClassLoader().getResourceAsStream("mariadb.properties")) {
            if (inputStream == null) {
              System.out.println("property file 'mariadb.properties' not found in the classpath");
            } else {
              Properties prop = new Properties();
              prop.load(inputStream);
              tmpVersion = prop.getProperty("version");
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
          instance = new Version(tmpVersion);
        }
      }
    }
    return instance;
  }
}
