// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.util;

public class Version {
  private final String version;
  private final int majorVersion;
  private final int minorVersion;
  private final int patchVersion;
  private final String qualifier;

  public Version(String versionString) {
    this.version = versionString;
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
    this.majorVersion = major;
    this.minorVersion = minor;
    this.patchVersion = patch;
    this.qualifier = qualif;
  }

  public String getVersion() {
    return version;
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

  public String getQualifier() {
    return qualifier;
  }

  /**
   * Utility method to check if database version is greater than parameters.
   *
   * @param major exact major version
   * @param minor exact minor version
   * @param patch patch version
   * @return true if version is greater than parameters
   */
  public boolean versionFixedMajorMinorGreaterOrEqual(int major, int minor, int patch) {
    if (this.majorVersion == major && this.minorVersion == minor && this.patchVersion >= patch) {
      return true;
    }
    return false;
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
}
