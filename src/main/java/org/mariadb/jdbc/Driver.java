// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

/** MariaDB Driver */
public final class Driver extends NonRegisteringDriver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      // eat
    }
  }
}
