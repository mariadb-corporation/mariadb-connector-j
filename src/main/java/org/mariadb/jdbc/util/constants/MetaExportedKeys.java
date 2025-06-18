// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.util.constants;

/** Indicate if metadata search must use Catalog or schema field. */
public enum MetaExportedKeys {
  /** Use information schema * */
  UseInformationSchema,

  /** Use SHOW CREATE command * */
  UseShowCreate,

  /** Use SHOW CREATE when local database, or IS when remote * */
  Auto;

  public static MetaExportedKeys from(String value) {
    for (MetaExportedKeys val : values()) {
      if (val.name().equalsIgnoreCase(value)) {
        return val;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for MetaExportedKeys", value));
  }
}
