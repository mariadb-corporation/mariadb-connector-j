// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mariadb.jdbc.util.ClientParser;

@SuppressWarnings("ConstantConditions")
public class ClientParserTest {

  private void parse(
      String sql,
      String[] expected,
      String[] expectedNoBackSlash,
      boolean isInsertDuplicate,
      boolean isMulti) {

    // perform test with regular parser
    ClientParser parser = ClientParser.parameterParts(sql, false);
    parse(parser, sql, expected, expectedNoBackSlash, isInsertDuplicate, isMulti);

    // perform test with rewritable parser
    ClientParser parser2 = ClientParser.rewritableParts(sql, false);
    parse(parser2, sql, expected, expectedNoBackSlash, isInsertDuplicate, isMulti);
  }

  private void parse(
      ClientParser parser,
      String sql,
      String[] expected,
      String[] expectedNoBackSlash,
      boolean isInsertDuplicate,
      boolean isMulti) {

    assertEquals(expected.length, parser.getParamCount() + 1, displayErr(parser, expected));

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(expected[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(
        expected[expected.length - 1],
        new String(parser.getQuery(), pos, parser.getQuery().length - pos));

    parser = ClientParser.parameterParts(sql, true);
    assertEquals(
        expectedNoBackSlash.length, parser.getParamCount() + 1, displayErr(parser, expected));
    pos = 0;
    paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      assertEquals(expectedNoBackSlash[i], new String(parser.getQuery(), pos, paramPos - pos));
      pos = paramPos + 1;
    }
    assertEquals(
        expectedNoBackSlash[expectedNoBackSlash.length - 1],
        new String(parser.getQuery(), pos, parser.getQuery().length - pos));

    assertEquals(isInsertDuplicate, parser.isInsertDuplicate());
    assertEquals(isMulti, parser.isMultiQuery());
  }

  private String displayErr(ClientParser parser, String[] exp) {
    StringBuilder sb = new StringBuilder();
    sb.append("is:\n");

    int pos = 0;
    int paramPos = parser.getQuery().length;
    for (int i = 0; i < parser.getParamCount(); i++) {
      paramPos = parser.getParamPositions().get(i);
      sb.append(new String(parser.getQuery(), pos, paramPos - pos, StandardCharsets.UTF_8))
          .append("\n");
      pos = paramPos + 1;
    }
    sb.append(
        new String(parser.getQuery(), pos, parser.getQuery().length - pos, StandardCharsets.UTF_8));

    sb.append("but was:\n");
    for (String s : exp) {
      sb.append(s).append("\n");
    }
    return sb.toString();
  }

  @Test
  public void ClientParser() {
    parse(
        "SELECT '\\\\test' /*test* #/ ;`*/",
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"},
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"},
        false,
        false);
    parse(
        "DO '\\\"', \"\\'\"",
        new String[] {"DO '\\\"', \"\\'\""},
        new String[] {"DO '\\\"', \"\\'\""},
        false,
        false);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2)",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2)"},
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2)"},
        false,
        false);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE KEY UPDATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE KEY UPDATE"},
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE KEY UPDATE"},
        true,
        false);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE"},
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ON DUPLICATE"},
        false,
        false);
    parse(
        "INSERT INTO TABLE(id,val) VALUES (1,2) ONDUPLICATE",
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ONDUPLICATE"},
        new String[] {"INSERT INTO TABLE(id,val) VALUES (1,2) ONDUPLICATE"},
        false,
        false);
  }

  @ParameterizedTest()
  @ValueSource(
      strings = {
        "DO 1; SELECT 1",
        "DO 1;\nDO 2",
      })
  public void MultiQueryParser(String sql) {
    assertTrue(ClientParser.parameterParts(sql, false).isMultiQuery());
    assertTrue(ClientParser.rewritableParts(sql, false).isMultiQuery());
  }

  @ParameterizedTest()
  @ValueSource(strings = {"DO 1;   ", "DO 1; \n ", "DO 1;"})
  public void NonMultiQueryParser(String sql) {
    assertFalse(ClientParser.parameterParts(sql, false).isMultiQuery());
    assertFalse(ClientParser.rewritableParts(sql, false).isMultiQuery());
  }

  @Test
  public void ClientParserInsertFlag() {
    assertFalse(ClientParser.parameterParts("WRONG INSERT_COMMAND", true).isInsert());
    assertFalse(ClientParser.parameterParts("INSERT_COMMAND WRONG ", true).isInsert());
    assertFalse(ClientParser.parameterParts("WRONGINSERT COMMAND", true).isInsert());
    assertFalse(ClientParser.parameterParts("WRONG INSERT", true).isInsert());
    assertFalse(ClientParser.parameterParts("WRONG small insert", true).isInsert());
    assertFalse(ClientParser.parameterParts("INSERT DUPLICATE", true).isInsertDuplicate());
    assertFalse(ClientParser.parameterParts("INSERT duplicate", true).isInsertDuplicate());
    assertFalse(ClientParser.parameterParts("INSERT _duplicate key", true).isInsertDuplicate());
    assertFalse(ClientParser.parameterParts("INSERT duplicate_ key", true).isInsertDuplicate());

    assertFalse(ClientParser.rewritableParts("WRONG INSERT_COMMAND", true).isInsert());
    assertFalse(ClientParser.rewritableParts("INSERT_COMMAND WRONG ", true).isInsert());
    assertFalse(ClientParser.rewritableParts("WRONGINSERT COMMAND", true).isInsert());
    assertFalse(ClientParser.rewritableParts("WRONG INSERT", true).isInsert());
    assertFalse(ClientParser.rewritableParts("WRONG small insert", true).isInsert());
    assertFalse(ClientParser.rewritableParts("INSERT DUPLICATE", true).isInsertDuplicate());
    assertFalse(ClientParser.rewritableParts("INSERT duplicate", true).isInsertDuplicate());
    assertFalse(ClientParser.rewritableParts("INSERT _duplicate key", true).isInsertDuplicate());
    assertFalse(ClientParser.rewritableParts("INSERT duplicate_ key", true).isInsertDuplicate());
  }

  /**
   * Test that '--' only starts a comment when followed by whitespace or control character.
   * Expressions like '2--1' (subtraction with negative number) should not treat '--' as comment
   * start.
   */
  @Test
  public void testDoubleDashNotCommentInExpression() {
    // '2--1' should NOT be a comment - the ? should be found as parameter
    parse(
        "SELECT 2--1, ?",
        new String[] {"SELECT 2--1, ", ""},
        new String[] {"SELECT 2--1, ", ""},
        false,
        false);
    // '-- ' (with space) should be a comment - comment ends at \n, so ? is found
    parse(
        "SELECT 1 -- comment\n, ?",
        new String[] {"SELECT 1 -- comment\n, ", ""},
        new String[] {"SELECT 1 -- comment\n, ", ""},
        false,
        false);
    // '--\t' (with tab) should be a comment - comment ends at \n, so ? is found
    parse(
        "SELECT 1 --\tcomment\n, ?",
        new String[] {"SELECT 1 --\tcomment\n, ", ""},
        new String[] {"SELECT 1 --\tcomment\n, ", ""},
        false,
        false);
    // '--' at end of query is a comment
    ClientParser parser = ClientParser.parameterParts("SELECT 1 --", false);
    assertEquals(0, parser.getParamCount());
  }

  /**
   * Test that after block comment ends, if next char is asterisk, it shouldn't start a new comment.
   * The lastChar must be reset to zero to avoid &quot;slash-star space star&quot; being parsed as
   * new comment.
   */
  @Test
  public void testBlockCommentFollowedByAsterisk() {
    // /* comment */* ? - the * after */ should not start a new comment
    parse(
        "/* comment */* ?",
        new String[] {"/* comment */* ", ""},
        new String[] {"/* comment */* ", ""},
        false,
        false);
    // Normal case: /* comment */ ? - should work as before
    parse(
        "/* comment */ ?",
        new String[] {"/* comment */ ", ""},
        new String[] {"/* comment */ ", ""},
        false,
        false);
    // /* */ /* */ - consecutive block comments should still work
    parse(
        "/* a */ /* b */ ?",
        new String[] {"/* a */ /* b */ ", ""},
        new String[] {"/* a */ /* b */ ", ""},
        false,
        false);
  }
}
