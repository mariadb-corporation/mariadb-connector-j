// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Driver;

public class EnquoteLiteralTest {

  @Test
  public void doublesQuote() {
    // JDBC spec: a single quote is escaped by doubling it
    assertEquals("'It''s a test'", Driver.enquoteLiteral("It's a test"));
    assertEquals("'good_$one'", Driver.enquoteLiteral("good_$one"));
  }

  @Test
  public void safeUnderBothSqlModes() {
    // every quote is doubled, so the literal cannot be closed early
    // (the backslash form 'x\' OR ...' would break out under NO_BACKSLASH_ESCAPES)
    assertEquals("'x'' OR ''1''=''1'", Driver.enquoteLiteral("x' OR '1'='1"));
    // a trailing backslash must be doubled, otherwise it escapes the closing quote
    // in the default sql_mode
    assertEquals("'a\\\\'", Driver.enquoteLiteral("a\\"));
    assertEquals("'\\\\'' OR 1=1 -- '", Driver.enquoteLiteral("\\' OR 1=1 -- "));
  }
}
