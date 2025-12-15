// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.export;

import java.sql.DataTruncation;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.MariaDbDataTruncation;

public class MariaDbDataTruncationTest {

  @Test
  public void testExceptionFactoryReturnDataTruncation() throws SQLException {
    Configuration conf = new Configuration.Builder().database("test").addHost("localhost", 3306).build();
    ExceptionFactory factory = new ExceptionFactory(conf, HostAddress.from("localhost", 3306));

    SQLException ex = factory.create("boom", "22003", 1264);

    assertTrue(ex instanceof DataTruncation);
    assertTrue(ex instanceof MariaDbDataTruncation);

    assertEquals("boom", ex.getMessage());
    assertEquals("22003", ex.getSQLState());
    assertEquals(1264, ex.getErrorCode());
  }
}
