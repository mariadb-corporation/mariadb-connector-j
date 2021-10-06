// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

public enum SslMode {
  // NO SSL
  DISABLE("disable", new String[] {"DISABLED", "0", "false"}),

  // Encryption only (no certificate and hostname validation) (DEVELOPMENT ONLY)
  TRUST("trust", new String[] {"REQUIRED"}),

  // Encryption, certificates validation, BUT no hostname verification
  VERIFY_CA("verify-ca", new String[] {"VERIFY_CA"}),

  // Standard SSL use: Encryption, certificate validation and hostname validation
  VERIFY_FULL("verify-full", new String[] {"VERIFY_IDENTITY", "1", "true"});

  private final String value;
  private final String[] aliases;

  SslMode(String value, String[] aliases) {
    this.value = value;
    this.aliases = aliases;
  }

  public static SslMode from(String value) {
    for (SslMode sslMode : values()) {
      if (sslMode.value.equalsIgnoreCase(value) || sslMode.name().equalsIgnoreCase(value)) {
        return sslMode;
      }
      for (String alias : sslMode.aliases) {
        if (alias.equalsIgnoreCase(value)) {
          return sslMode;
        }
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for SslMode", value));
  }
}
