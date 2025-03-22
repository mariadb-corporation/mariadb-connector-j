// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;

public class Ttest extends Common {
  @Test
  public void tt() throws SQLException {

    DecimalFormat df = new DecimalFormat(".0#####", DecimalFormatSymbols.getInstance(Locale.US));
    String result = df.format(0.212345);
    System.out.println(result);
  }

  @Test
  public void tt2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS tt");
    stmt.execute("CREATE TABLE tt(t BLOB)");
    stmt.execute("INSERT INTO tt(t) VALUE ('t')");

    try (PreparedStatement prep = sharedConn.prepareStatement("SELECT * FROM tt")) {
      ResultSet rs = prep.executeQuery();
      rs.next();
      Object obj = rs.getObject(1, Blob.class);
      System.out.println(obj);
      System.out.println(rs.getString(1) + " " + rs.getTimestamp(1));
    }
  }
}
