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

package org.mariadb.jdbc.client;

public final class ServerVersion {

  private final String serverVersion;
  private final int majorVersion;
  private final int minorVersion;
  private final int patchVersion;
  private final boolean mariaDBServer;

  public ServerVersion(String serverVersion, boolean mariaDBServer) {
    this.serverVersion = serverVersion;
    this.mariaDBServer = mariaDBServer;
    int[] parsed = parseVersion(serverVersion);
    this.majorVersion = parsed[0];
    this.minorVersion = parsed[1];
    this.patchVersion = parsed[2];
  }

  public boolean isMariaDBServer() {
    return mariaDBServer;
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public int getPatchVersion() {
    return patchVersion;
  }

  public String getServerVersion() {
    return serverVersion;
  }

  /**
   * Utility method to check if database version is greater than parameters.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true if version is greater than parameters
   */
  public boolean versionGreaterOrEqual(int major, int minor, int patch) {
    if (this.majorVersion > major) {
      return true;
    }

    if (this.majorVersion < major) {
      return false;
    }

    /*
     * Major versions are equal, compare minor versions
     */
    if (this.minorVersion > minor) {
      return true;
    }
    if (this.minorVersion < minor) {
      return false;
    }

    // Minor versions are equal, compare patch version.
    return this.patchVersion >= patch;
  }

  private int[] parseVersion(String serverVersion) {
    int length = serverVersion.length();
    char car;
    int offset = 0;
    int type = 0;
    int val = 0;
    int majorVersion = 0;
    int minorVersion = 0;
    int patchVersion = 0;

    main_loop:
    for (; offset < length; offset++) {
      car = serverVersion.charAt(offset);
      if (car < '0' || car > '9') {
        switch (type) {
          case 0:
            majorVersion = val;
            break;
          case 1:
            minorVersion = val;
            break;
          case 2:
            patchVersion = val;
            break main_loop;
          default:
            break;
        }
        type++;
        val = 0;
      } else {
        val = val * 10 + car - 48;
      }
    }

    // serverVersion finished by number like "5.5.57", assign patchVersion
    if (type == 2) {
      patchVersion = val;
    }
    return new int[] {majorVersion, minorVersion, patchVersion};
  }
}
