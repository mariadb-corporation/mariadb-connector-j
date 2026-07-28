// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;

public class ConnectionHelperTest {

  @Test
  public void assertValidClassNameAcceptsRealClassNames() {
    for (String ok :
        new String[] {
          "org.example.MyFactory",
          "com.foo.Bar$Baz",
          "SingleName",
          "_leadingUnderscore",
          "org.mariadb.jdbc.integration.util.SocketFactoryBasicTest"
        }) {
      assertDoesNotThrow(() -> ConnectionHelper.assertValidClassName(ok), ok);
    }
  }

  @Test
  public void assertValidClassNameRejectsResourceLocators() {
    // the report's vector, plus the shapes a class-name-to-path reconstruction could smuggle
    for (String bad :
        new String[] {
          "jar:file:/proc/self/fd/42!/Evil",
          "jar:file:.proc.self.fd.42!.Evil",
          "http://attacker/x.jar",
          "/etc/passwd",
          "a/b/c",
          "a b",
          "",
          ".leadingDot",
          "trailingDot.",
          "double..dot"
        }) {
      IOException e =
          assertThrows(
              IOException.class,
              () -> ConnectionHelper.assertValidClassName(bad),
              "expected: " + bad);
      assertTrue(e.getMessage().contains("not a valid class name"), e.getMessage());
    }
  }

  @Test
  public void assertValidClassNameRejectsIgnorableAndUnicodeChars() {
    // Character.isJavaIdentifier* accepts these, but they are never part of a real class name:
    // null byte, control char, zero-width space (format char), and non-ASCII letter.
    for (String bad :
        new String[] {
          "com.Foo" + (char) 0x00 + "Bar", // null byte
          "com.Foo" + (char) 0x01 + "Bar", // control char
          "com.Foo" + (char) 0x200B + "Bar", // zero-width space
          "com.Foo" + (char) 0x00E9 // non-ASCII letter (é)
        }) {
      IOException e =
          assertThrows(
              IOException.class,
              () -> ConnectionHelper.assertValidClassName(bad),
              "expected rejection");
      assertTrue(e.getMessage().contains("not a valid class name"), e.getMessage());
    }
  }

  @Test
  public void standardSocketRejectsUrlSocketFactoryBeforeConnecting() {
    // a value that is not a class name must be refused at the sink, before any socket is opened;
    // 127.0.0.1:1 would fail to connect, so a thrown "not a valid class name" proves the guard
    // ran first rather than a connection error.
    Configuration conf =
        new Configuration.Builder()
            .socketFactory("jar:file:/proc/self/fd/42!/Evil")
            .addHost("127.0.0.1", 1)
            .build();
    IOException e =
        assertThrows(
            IOException.class,
            () -> ConnectionHelper.standardSocket(conf, HostAddress.from("127.0.0.1", 1)));
    assertTrue(e.getMessage().contains("not a valid class name"), e.getMessage());
  }
}
