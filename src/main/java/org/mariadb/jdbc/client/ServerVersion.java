// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

public interface ServerVersion {
  /**
   * Get server version string
   *
   * @return server version string
   */
  String getVersion();

  /**
   * get server major version, parsed from server version string
   *
   * @return server major version
   */
  int getMajorVersion();

  /**
   * get server minor version, parsed from server version string
   *
   * @return server minor version
   */
  int getMinorVersion();

  /**
   * get server patch version, parsed from server version string
   *
   * @return server patch version
   */
  int getPatchVersion();

  /**
   * get server qualifier, parsed from server version string
   *
   * @return server qualifier
   */
  String getQualifier();

  /**
   * Utility method to check if database version is greater than parameters.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true if version is greater than parameters
   */
  boolean versionGreaterOrEqual(int major, int minor, int patch);

  /**
   * Is server mariadb
   *
   * @return true if server is a MariaDB server
   */
  boolean isMariaDBServer();
}
