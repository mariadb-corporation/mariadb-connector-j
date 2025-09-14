// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mariadb.jdbc.util.ClientParser;

public class ClientParserRewritableTest {

  /** SELECT query cannot be rewritable. */
  @Test
  public void selectQuery() {
    // SELECT query cannot be rewritable
    assertFalse(checkRewritable("SELECT * FROM MyTable", 0, 0));
    assertFalse(checkRewritable("SELECT\n * FROM MyTable", 0, 0));
    assertFalse(checkRewritable("SELECT(1)", 0, 0));
    assertFalse(checkRewritable("INSERT MyTable (a) VALUES (1);SELECT(1)", 0, 0));
  }

  /** INSERT FROM SELECT are not be rewritable. */
  @Test
  public void insertSelectQuery() {
    assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT * FROM seq_1_to_1000", 0, 0));
    assertFalse(checkRewritable("INSERT INTO MyTable (a);SELECT * FROM seq_1_to_1000", 0, 0));
    assertFalse(checkRewritable("INSERT INTO MyTable (a)SELECT * FROM seq_1_to_1000", 0, 0));
    assertFalse(checkRewritable("INSERT INTO MyTable (a) (SELECT * FROM seq_1_to_1000)", 0, 0));
    assertFalse(checkRewritable("INSERT INTO MyTable (a) SELECT\n * FROM seq_1_to_1000", 0, 0));
  }

  /** If parameters exist outside the VALUES() block, not rewritable. */
  @Test
  public void insertParametersOutsideValues() {
    assertFalse(
        checkRewritable("INSERT INTO TABLE(col1) VALUES (?) ON DUPLICATE KEY UPDATE col2=?", 0, 0));
  }

  /** LAST_INSERT_ID is not rewritable. */
  @Test
  public void insertLastInsertId() {
    assertFalse(
        checkRewritable("INSERT INTO TABLE(col1, col2) VALUES (?, LAST_INSERT_ID())", 0, 0));
  }

  /**
   * Insert query that contain table/column name with select keyword, or select in comment can be
   * rewritten.
   */
  @Test
  public void rewritableThatContainSelectQuery() {
    // but 'SELECT' keyword in column/table name can be rewritable
    assertTrue(checkRewritable("INSERT INTO TABLE_SELECT VALUES (?)", 32, 34));
    assertTrue(checkRewritable("INSERT INTO TABLE_SELECT VALUES (?)", 32, 34));
    assertTrue(checkRewritable("INSERT INTO SELECT_TABLE VALUES (?)", 32, 34));
    assertTrue(checkRewritable("INSERT INTO `TABLE SELECT ` VALUES (?)", 35, 37));
    assertTrue(checkRewritable("INSERT INTO TABLE /* SELECT in comment */  VALUES (?)", 50, 52));
    assertTrue(checkRewritable("INSERT INTO TABLE  VALUES (?) //SELECT", 26, 28));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES ('abc', ?)", 25, 34));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES (\"a''bc\", ?)", 25, 36));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES ('\\\\test', ?) /*test* #/ ;`*/", 25, 37));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES ('\\\\test', ?) # EOL ", 25, 37));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES ('\\\\test', ?) -- EOL ", 25, 37));
    assertTrue(checkRewritable("INSERT INTO TABLE VALUES ('\\\\test', ?) -- EOL ()", 25, 37));
  }

  private boolean checkRewritable(String query, int pos1, int pos2) {
    List<Integer> valuesBracketPositions =
        ClientParser.rewritableParts(query, true).getValuesBracketPositions();
    if (valuesBracketPositions == null) {
      return false;
    } else if (valuesBracketPositions.size() == 2) {
      assertEquals(pos1, valuesBracketPositions.get(0));
      assertEquals(pos2, valuesBracketPositions.get(1));
      return true;
    } else {
      fail("valuesBracketPositions().size() != 2");
      return false; // appeasing the compiler: this line will never be executed.
    }
  }

  static Stream<Arguments> rewriteTestData() {
    return Stream.of(
        Arguments.of("INSERT INTO b VALUES (?)", 1, new int[] {22}, new int[] {21, 23}),
        Arguments.of("UPDATE b set a=?", 1, new int[] {15}, null),
        Arguments.of("INSERT INTO b VALUES (?,? )", 2, new int[] {22, 24}, new int[] {21, 26}),
        Arguments.of("INSERT INTO b (SELECT a FROM b where c=? )", 1, new int[] {39}, null),
        Arguments.of(
            "INSERT INTO b VALUES (?,?), (?,?)",
            4,
            new int[] {22, 24, 29, 31},
            null),
    	Arguments.of(
            "INSERT INTO b VALUES (?,?) AS v ON DUPLICATE KEY UPDATE b.a=v.a",
            2,
            new int[] {22, 24},
            new int[] {21, 25}),
        Arguments.of(
            "INSERT INTO b VALUES (?,?) AS v ON DUPLICATE KEY UPDATE b.a=(v.a)",
            2,
            new int[] {22, 24},
            new int[] {21, 25}));
  }

  @ParameterizedTest()
  @MethodSource("rewriteTestData")
  public void rewritableParser(
      String sql, int paramCount, int[] paramPosition, int[] valuesBracketPositions) {
    ClientParser parser = ClientParser.rewritableParts(sql, false);
    assertEquals(parser.getSql(), sql);
    assertEquals(parser.getParamCount(), paramCount);
    assertArrayEquals(
        parser.getParamPositions().stream().mapToInt(Integer::intValue).toArray(), paramPosition);
    if (valuesBracketPositions == null) {
      assertNull(parser.getValuesBracketPositions());
    } else {
      assertArrayEquals(
          parser.getValuesBracketPositions().stream().mapToInt(Integer::intValue).toArray(),
          valuesBracketPositions);
    }
  }
}
