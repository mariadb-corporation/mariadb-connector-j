// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.ClientParser;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class ClientPreparedStatementParsingTest extends Common {

  private void checkParsing(String sql, int paramNumber, String[] partsMulti) {
    boolean noBackslashEscapes =
        (sharedConn.getContext().getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;
    ClientParser parser = ClientParser.parameterParts(sql, noBackslashEscapes);
    assertEquals(paramNumber, parser.getParamCount());
    int pos = 0;
    int paramPos;
    for (int i = 0; i < parser.getParamPositions().size(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(partsMulti[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(
        partsMulti[partsMulti.length - 1],
        new String(parser.getQuery(), pos, parser.getQuery().length - pos));
  }

  @Test
  public void stringEscapeParsing() throws Exception {
    checkParsing(
        "select '\\'' as a, ? as b, \"\\\"\" as c, ? as d",
        2,
        new String[] {"select '\\'' as a, ", " as b, \"\\\"\" as c, ", " as d"});
  }

  @Test
  public void testRewritableWithConstantParameter() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY"
            + " UPDATE col2=col2+10",
        2,
        new String[] {
          "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
          ", 5, ",
          ", 8) ON DUPLICATE KEY UPDATE col2=col2+10"
        });
  }

  @Test
  public void testComment() throws Exception {
    checkParsing(
        "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " INSERT into "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " tt VALUES "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
            + " (?) "
            + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */",
        1,
        new String[] {
          "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " INSERT into "
              + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " tt VALUES "
              + "/* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
              + " (",
          ") /* insert Select INSERT INTO tt VALUES (?,?,?,?)  */"
        });
  }

  @Test
  public void testRewritableWithConstantParameterAndParamAfterValue() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE KEY"
            + " UPDATE col2=?",
        3,
        new String[] {
          "INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ",
          ", 5, ",
          ", 8) ON DUPLICATE KEY UPDATE col2=",
          ""
        });
  }

  @Test
  public void testRewritableMultipleInserts() throws Exception {
    checkParsing(
        "INSERT INTO TABLE(col1,col2) VALUES (?, ?), (?, ?)",
        4,
        new String[] {"INSERT INTO TABLE(col1,col2) VALUES (", ", ", "), (", ", ", ")"});
  }

  @Test
  public void testCall() throws Exception {
    checkParsing("CALL dsdssd(?,?)", 2, new String[] {"CALL dsdssd(", ",", ")"});
  }

  @Test
  public void testUpdate() throws Exception {
    checkParsing(
        "UPDATE MultiTestt4 SET test = ? WHERE test = ?",
        2,
        new String[] {"UPDATE MultiTestt4 SET test = ", " WHERE test = ", ""});
  }

  @Test
  public void testInsertSelect() throws Exception {
    checkParsing(
        "insert into test_insert_select ( field1) (select  TMP.field1 from "
            + "(select CAST(? as binary) `field1` from dual) TMP)",
        1,
        new String[] {
          "insert into test_insert_select ( field1) (select  TMP.field1 from (select CAST(",
          " as binary) `field1` from dual) TMP)"
        });
  }

  @Test
  public void testWithoutParameter() throws Exception {
    checkParsing("SELECT testFunction()", 0, new String[] {"SELECT testFunction()"});
  }

  @Test
  public void testWithoutParameterAndParenthesis() throws Exception {
    checkParsing("SELECT 1", 0, new String[] {"SELECT 1"});
  }

  @Test
  public void testWithoutParameterAndValues() throws Exception {
    checkParsing("INSERT INTO tt VALUES (1)", 0, new String[] {"INSERT INTO tt VALUES (1)"});
  }

  @Test
  public void testSemiColon() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
        1,
        new String[] {
          "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')"
        });
  }

  @Test
  public void testSemicolonRewritableIfAtEnd() throws Exception {
    checkParsing(
        "INSERT INTO table (column1) VALUES (?); ",
        1,
        new String[] {"INSERT INTO table (column1) VALUES (", "); "});
  }

  @Test
  public void testSemicolonNotRewritableIfNotAtEnd() throws Exception {
    checkParsing(
        "INSERT INTO table (column1) VALUES (?); SELECT 1",
        1,
        new String[] {"INSERT INTO table (column1) VALUES (", "); SELECT 1"});
  }

  @Test
  public void testError() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?); INSERT INTO tt (tt) VALUES ('multiple')",
        1,
        new String[] {
          "INSERT INTO tt (tt) VALUES (", "); INSERT INTO tt (tt) VALUES ('multiple')"
        });
  }

  @Test
  public void testLineComment() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES (?) --fin",
        1,
        new String[] {"INSERT INTO tt (tt) VALUES (", ") --fin"});
  }

  @Test
  public void testLineCommentFinished() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt) VALUES --fin\n (?)",
        1,
        new String[] {"INSERT INTO tt (tt) VALUES --fin\n (", ")"});
  }

  @Test
  public void testSelect1() throws Exception {
    checkParsing("SELECT 1", 0, new String[] {"SELECT 1"});
  }

  @Test
  public void rewriteBatchedError() throws Exception {
    try (Connection connection =
        createCon("&rewriteBatchedStatements=true&dumpQueriesOnException")) {
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO errorTable (a, b) VALUES (?, ?, ?)");

      preparedStatement.setString(1, "1");
      preparedStatement.setString(2, "2");
      preparedStatement.setString(3, "3");
      preparedStatement.addBatch();
      try {
        preparedStatement.executeBatch();
        fail("must have thrown error since parameters are not good");
      } catch (SQLException e) {
        assertTrue(
            e.getCause()
                .getMessage()
                .contains("Query is: INSERT INTO errorTable (a, b) VALUES (?, ?, ?)"));
      }
    }
  }

  @Test
  public void errorException() throws Exception {
    try (Connection connection = createCon("dumpQueriesOnException")) {
      ensureErrorException(connection);
    }
  }

  private void ensureErrorException(Connection connection) throws SQLException {
    connection.createStatement().execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement pstmt =
        connection.prepareStatement("UPDATE unknownTable SET col1 = ?, col2 = 0 WHERE col3 = ?");
    pstmt.setInt(1, 10);
    pstmt.setInt(2, 20);
    pstmt.addBatch();
    pstmt.setInt(1, 100);
    pstmt.setInt(2, 200);
    try {
      pstmt.executeBatch();
      fail("Must have thrown error");
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("doesn't exist")
              || sqle.getMessage().contains("Relation not found: \"testj\".\"unknownTable\"")
              || sqle.getMessage().contains("Unknown prepared statement handler"),
          sqle.getMessage());
      assertTrue(
          sqle.getMessage()
              .contains("Query is: UPDATE unknownTable SET col1 = ?, col2 = 0 WHERE col3 = ?"),
          sqle.getMessage());
    }
  }

  @Test
  public void testLastInsertId() throws Exception {
    checkParsing(
        "INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ?)",
        1,
        new String[] {"INSERT INTO tt (tt, tt2) VALUES (LAST_INSERT_ID(), ", ")"});
  }

  @Test
  public void testValuesForPartition() throws Exception {
    checkParsing(
        "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
            + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01'))",
        0,
        new String[] {
          "ALTER table test_partitioning PARTITION BY RANGE COLUMNS( created_at ) "
              + "(PARTITION test_p201605 VALUES LESS THAN ('2016-06-01'))"
        });
  }

  @Test
  public void testParse() throws Exception {
    checkParsing(
        "INSERT INTO `myTable` VALUES ('\\n"
            + "\"\\'', \"'\\\n"
            + "\\\"\")  \n"
            + " // comment\n"
            + " , ('a', 'b') # EOL comment",
        0,
        new String[] {
          "INSERT INTO `myTable` VALUES ('\\n"
              + "\"\\'', \"'\\\n"
              + "\\\"\")  \n"
              + " // comment\n"
              + " , ('a', 'b') # EOL comment"
        });
  }
}
