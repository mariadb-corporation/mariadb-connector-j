// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.util.constants;

/** Indicate if metadata search must use Catalog or schema field. */
public enum CatalogTerm {
  /** Use catalog field for metadata * */
  UseCatalog,

  /** Use schema field for metadata * */
  UseSchema
}
