// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.util;

import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class VersionFactory {
  private static Version instance = null;

  // use getShape method to get object of type shape
  public static Version getInstance() {
    Logger logger = Loggers.getLogger(VersionFactory.class);
    if (instance == null) {
      synchronized (VersionFactory.class) {
        if (instance == null) {
          String tmpVersion = "unknown";
          try (InputStream inputStream =
              Version.class.getClassLoader().getResourceAsStream("singlestore.properties")) {
            if (inputStream == null) {
              logger.warn("Property file 'singlestore.properties' not found in the classpath");
            } else {
              Properties prop = new Properties();
              prop.load(inputStream);
              tmpVersion = prop.getProperty("version");
            }
          } catch (IOException e) {
            logger.warn("Failed to retrieve driver version: " + e.getMessage());
          }
          instance = new Version(tmpVersion);
        }
      }
    }
    return instance;
  }
}
