// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.util.ClientParser;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
public class ClientParserTest {

  private void parse(String sql, String[] expected, String[] expectedNoBackSlash) {
    ClientParser parser = ClientParser.parameterParts(sql, false);
    assertEquals(expected.length, parser.getQueryParts().size(), displayErr(parser, expected));
    for (int i = 0; i < parser.getQueryParts().size(); i++) {
      byte[] b = parser.getQueryParts().get(i);
      assertEquals(expected[i], new String(b, StandardCharsets.UTF_8));
    }

    parser = ClientParser.parameterParts(sql, true);
    assertEquals(
        expectedNoBackSlash.length, parser.getQueryParts().size(), displayErr(parser, expected));
    for (int i = 0; i < parser.getQueryParts().size(); i++) {
      byte[] b = parser.getQueryParts().get(i);
      assertEquals(expectedNoBackSlash[i], new String(b, StandardCharsets.UTF_8));
    }
  }

  private String displayErr(ClientParser parser, String[] exp) {
    StringBuilder sb = new StringBuilder();
    sb.append("is:\n");
    for (byte[] b : parser.getQueryParts()) {
      sb.append(new String(b, StandardCharsets.UTF_8)).append("\n");
    }
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
        new String[] {"SELECT '\\\\test' /*test* #/ ;`*/"});
    parse(
        "DO '\\\"', \"\\'\"",
        new String[] {"DO '\\\"', \"\\'\""},
        new String[] {"DO '\\\"', \"\\'\""});
  }
}
