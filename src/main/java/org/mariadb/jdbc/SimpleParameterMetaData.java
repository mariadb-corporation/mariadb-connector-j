/*
 *
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

package org.mariadb.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionFactory;

/** Very basic info about the parameterized query, only reliable method is getParameterCount(). */
public class SimpleParameterMetaData implements ParameterMetaData {

  private final int parameterCount;

  public SimpleParameterMetaData(int parameterCount) {
    this.parameterCount = parameterCount;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return parameterCount;
  }

  @Override
  public int isNullable(final int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    return ParameterMetaData.parameterNullableUnknown;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    return true;
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata precision");
  }

  @Override
  public int getScale(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata scale");
  }

  /**
   * Parameter type are not sent by server. See https://jira.mariadb.org/browse/CONJ-568 and
   * https://jira.mariadb.org/browse/MDEV-15031
   *
   * @param param parameter number
   * @return SQL type from java.sql.Types
   * @throws SQLException a feature not supported, since server doesn't sent the right information
   */
  @Override
  public int getParameterType(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    throw ExceptionFactory.INSTANCE.notSupported(
        "Getting parameter type metadata are not supported");
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata type name");
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    if (param < 1 || param > parameterCount) {
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "Parameter metadata out of range : param was %s and must be in range 1 - %s",
              param, parameterCount),
          "07009");
    }
    throw ExceptionFactory.INSTANCE.create("Unknown parameter metadata class name");
  }

  @Override
  public int getParameterMode(int param) {
    return parameterModeIn;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return iface.cast(this);
      } else {
        throw new SQLException("The receiver is not a wrapper for " + iface.getName());
      }
    } catch (Exception e) {
      throw new SQLException("The receiver is not a wrapper and does not implement the interface");
    }
  }

  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
