// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.singlestore.jdbc.Configuration;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code preserveInstants} connection option. These tests do not require a
 * running SingleStore server.
 *
 * <p>The option aligns with the same-named option in MySQL Connector/J (default {@code true} since
 * 8.0.23) and MariaDB Connector/J. When enabled, {@code OffsetDateTime} parameters bound to
 * TIMESTAMP / DATETIME columns are serialized as {@code FROM_UNIXTIME(epoch)} SQL literals in
 * {@code encodeText}, preserving the absolute UTC instant regardless of the server's
 * {@code @@session.time_zone} interpretation.
 */
public class PreserveInstantsConfigurationTest {

  @Test
  public void defaultsToFalseForBackwardsCompatibility() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test");
    assertFalse(
        conf.preserveInstants(),
        "preserveInstants must default to false for backwards compatibility");
  }

  @Test
  public void enabledViaUrlParameter() throws SQLException {
    Configuration conf =
        Configuration.parse("jdbc:singlestore://localhost/test?preserveInstants=true");
    assertTrue(conf.preserveInstants(), "preserveInstants=true in URL must be honored");
  }

  @Test
  public void disabledExplicitlyViaUrl() throws SQLException {
    Configuration conf =
        Configuration.parse("jdbc:singlestore://localhost/test?preserveInstants=false");
    assertFalse(conf.preserveInstants(), "preserveInstants=false in URL must be honored");
  }

  @Test
  public void enabledViaProperties() throws SQLException {
    Properties props = new Properties();
    props.setProperty("preserveInstants", "true");
    Configuration conf = Configuration.parse("jdbc:singlestore://localhost/test", props);
    assertTrue(conf.preserveInstants(), "preserveInstants=true in Properties must be honored");
  }

  @Test
  public void enabledViaBuilder() throws SQLException {
    Configuration conf =
        new Configuration.Builder().database("test").preserveInstants(true).build();
    assertTrue(conf.preserveInstants(), "Builder.preserveInstants(true) must be honored");
  }

  @Test
  public void disabledViaBuilder() throws SQLException {
    Configuration conf =
        new Configuration.Builder().database("test").preserveInstants(false).build();
    assertFalse(conf.preserveInstants(), "Builder.preserveInstants(false) must be honored");
  }

  @Test
  public void combinedWithRewriteBatchedStatements() throws SQLException {
    Configuration conf =
        Configuration.parse(
            "jdbc:singlestore://localhost/test?rewriteBatchedStatements=true&preserveInstants=true");
    assertTrue(conf.rewriteBatchedStatements());
    assertTrue(conf.preserveInstants());
  }
}
