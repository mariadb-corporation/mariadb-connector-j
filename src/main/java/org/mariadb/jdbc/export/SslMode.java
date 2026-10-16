// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.export;

/** SSl requirement */
public enum SslMode {
  /** NO SSL */
  DISABLE("disable", new String[] {"DISABLED", "0", "false"}),

  /** Encryption only (no certificate and hostname validation) (DEVELOPMENT ONLY) */
  TRUST("trust", new String[] {"REQUIRED"}),

  /** Encryption, certificates validation, BUT no hostname verification */
  VERIFY_CA("verify-ca", new String[] {"VERIFY_CA"}),

  /** Standard SSL use: Encryption, certificate validation and hostname validation */
  VERIFY_FULL("verify-full", new String[] {"VERIFY_IDENTITY", "1", "true"}),

  /**
   * VERIFY_FULL when the server announces TLS, no encryption otherwise. For a mix of servers with
   * and without TLS behind one connection string: the choice is made from the capability flags the
   * server sends in clear, so an active attacker can downgrade the connection; a migration mode, to
   * be replaced by VERIFY_FULL once every server has TLS.
   */
  ALLOW("allow", new String[] {});

  private final String value;
  private final String[] aliases;

  SslMode(String value, String[] aliases) {
    this.value = value;
    this.aliases = aliases;
  }

  public String getValue() {
    return value;
  }

  /**
   * Create SSLMode from enumeration value, or aliases
   *
   * @param value enumeration value or alias
   * @return SSLMode if corresponding enumeration value or alias found
   */
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
