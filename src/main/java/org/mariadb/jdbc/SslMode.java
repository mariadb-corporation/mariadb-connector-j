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

package org.mariadb.jdbc;

public enum SslMode {
  // NO SSL
  DISABLE("disable", "DISABLED"),

  // Encryption only (no certificate and hostname validation) (DEVELOPMENT ONLY)
  TRUST("trust", "REQUIRED"),

  // Encryption, certificates validation, BUT no hostname verification
  VERIFY_CA("verify-ca", "VERIFY_CA"),

  // Standard SSL use: Encryption, certificate validation and hostname validation
  VERIFY_FULL("verify-full", "VERIFY_IDENTITY");

  private final String value;
  private final String alias;

  SslMode(String value, String alias) {
    this.value = value;
    this.alias = alias;
  }

  public static SslMode from(String value) {
    for (SslMode sslMode : values()) {
      if (sslMode.value.equalsIgnoreCase(value)
          || sslMode.name().equalsIgnoreCase(value)
          || sslMode.alias.equalsIgnoreCase(value)) {
        return sslMode;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for SslMode", value));
  }
}
