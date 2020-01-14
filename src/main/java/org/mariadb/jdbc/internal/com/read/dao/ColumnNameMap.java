/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.read.dao;

import org.mariadb.jdbc.internal.com.read.resultset.ColumnInformation;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ColumnNameMap {

  private final ColumnInformation[] columnInfo;
  private Map<String, Integer> originalMap;
  private Map<String, Integer> aliasMap;

  public ColumnNameMap(ColumnInformation[] columnInformations) {
    this.columnInfo = columnInformations;
  }

  /**
   * Get column index by name.
   *
   * @param name column name
   * @return index.
   * @throws SQLException if no column info exists, or column is unknown
   */
  public int getIndex(String name) throws SQLException {
    if (name == null) {
      throw new SQLException("Column name cannot be null");
    }
    String lowerName = name.toLowerCase(Locale.ROOT);
    // The specs in JDBC 4.0 specify that ResultSet.findColumn and
    // ResultSet.getXXX(String name) should use column alias (AS in the query). If label is not
    // found, we use
    // original table name.
    if (aliasMap == null) {
      aliasMap = new HashMap<>();
      int counter = 0;
      for (ColumnInformation ci : columnInfo) {
        String columnAlias = ci.getName();
        if (columnAlias != null) {
          columnAlias = columnAlias.toLowerCase(Locale.ROOT);
          aliasMap.putIfAbsent(columnAlias, counter);

          String tableName = ci.getTable();
          if (tableName != null) {
            aliasMap.putIfAbsent(tableName.toLowerCase(Locale.ROOT) + "." + columnAlias, counter);
          }
        }
        counter++;
      }
    }

    Integer res = aliasMap.get(lowerName);
    if (res != null) {
      return res;
    }

    if (originalMap == null) {
      originalMap = new HashMap<>();
      int counter = 0;
      for (ColumnInformation ci : columnInfo) {
        String columnRealName = ci.getOriginalName();
        if (columnRealName != null) {
          columnRealName = columnRealName.toLowerCase(Locale.ROOT);
          originalMap.putIfAbsent(columnRealName, counter);

          String tableName = ci.getOriginalTable();
          if (tableName != null) {
            originalMap.putIfAbsent(
                tableName.toLowerCase(Locale.ROOT) + "." + columnRealName, counter);
          }
        }
        counter++;
      }
    }

    res = originalMap.get(lowerName);

    if (res == null) {
      throw ExceptionMapper.get("No such column: " + name, "42S22", 1054, null, false);
    }
    return res;
  }
}
