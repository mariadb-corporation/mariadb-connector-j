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

package org.mariadb.jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Version {
  public static final String version;
  public static final int majorVersion;
  public static final int minorVersion;
  public static final int patchVersion;
  public static final String qualifier;

  static {
    InputStream inputStream = null;
    String tmpVersion = "5.5.0";
    try {
      Properties prop = new Properties();
      inputStream = Version.class.getClassLoader().getResourceAsStream("mariadb.properties");
      if (inputStream != null) {
        prop.load(inputStream);
      } else {
        System.out.println("property file 'mariadb.properties' not found in the classpath");
      }

      // get the property value and print it out
      tmpVersion = prop.getProperty("version");
    } catch (Exception e) {
      // eat
    } finally {
      try {
        inputStream.close();
      } catch (IOException ioe) {
        // eat
      }
    }
    version = tmpVersion;
    int major = 0;
    int minor = 0;
    int patch = 0;
    String qualif = "";

    int length = version.length();
    char car;
    int offset = 0;
    int type = 0;
    int val = 0;
    for (; offset < length; offset++) {
      car = version.charAt(offset);
      if (car < '0' || car > '9') {
        switch (type) {
          case 0:
            major = val;
            break;
          case 1:
            minor = val;
            break;
          case 2:
            patch = val;
            qualif = version.substring(offset);
            offset = length;
            break;
          default:
            break;
        }
        type++;
        val = 0;
      } else {
        val = val * 10 + car - 48;
      }
    }

    if (type == 2) {
      patch = val;
    }
    majorVersion = major;
    minorVersion = minor;
    patchVersion = patch;
    qualifier = qualif;
  }
}
