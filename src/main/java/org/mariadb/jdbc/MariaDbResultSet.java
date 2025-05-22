// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.ResultSet;

public interface MariaDbResultSet extends ResultSet {
  int TYPE_SEQUENTIAL_ACCESS_ONLY = 2003;
}
