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

  @Test
  public void defaultMode() {
    // no-arg form assumes the default sql_mode (backslash is an escape char)
    assertEquals(Driver.enquoteLiteral("a\\b"), Driver.enquoteLiteral("a\\b", false));
    // default mode: backslash doubled, double-quote and control chars backslash-escaped
    assertEquals("'a\\\\b'", Driver.enquoteLiteral("a\\b", false));
    assertEquals("'he \\\"q\\\"'", Driver.enquoteLiteral("he \"q\"", false));
    assertEquals("'a\\nb'", Driver.enquoteLiteral("a\nb", false));
  }

  @Test
  public void noBackslashEscapesMode() {
    // NO_BACKSLASH_ESCAPES: backslash is a literal char, so it is NOT doubled ...
    assertEquals("'a\\b'", Driver.enquoteLiteral("a\\b", true));
    // ... and a trailing backslash is safe without doubling (it escapes nothing)
    assertEquals("'a\\'", Driver.enquoteLiteral("a\\", true));
    // double-quote and control chars are left untouched (only the quote is special)
    assertEquals("'he \"q\"'", Driver.enquoteLiteral("he \"q\"", true));
    assertEquals("'a\nb'", Driver.enquoteLiteral("a\nb", true));
    // the quote is still doubled, so the literal cannot be closed early -> injection-safe
    assertEquals("'x'' OR ''1''=''1'", Driver.enquoteLiteral("x' OR '1'='1", true));
    assertEquals("'\\'' OR 1=1 -- '", Driver.enquoteLiteral("\\' OR 1=1 -- ", true));
  }
}
