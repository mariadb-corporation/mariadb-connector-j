// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.util;

public final class Version {
  private final String version;
  private final int majorVersion;
  private final int minorVersion;
  private final int patchVersion;
  private final String qualifier;

  public Version(
      String version, int majorVersion, int minorVersion, int patchVersion, String qualifier) {
    this.version = version;
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.patchVersion = patchVersion;
    this.qualifier = qualifier;
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
}
