// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.message;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.message.ClientMessage;

public class ClientMessageTest {

  @Test
  public void validateLocalFileNameCaseSensitive() {
    String sql = "LOAD DATA LOCAL INFILE '/tmp/foo.txt' INTO TABLE t";

    // file requested by server matches the query
    Assertions.assertTrue(ClientMessage.validateLocalFileName(sql, null, "/tmp/foo.txt", null));

    // server asks for a path differing only in case : distinct file on case-sensitive
    // filesystems, must be refused
    Assertions.assertFalse(ClientMessage.validateLocalFileName(sql, null, "/tmp/FOO.txt", null));
    Assertions.assertFalse(ClientMessage.validateLocalFileName(sql, null, "/tmp/foo.TXT", null));

    // statement keywords stay case-insensitive
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "load data local infile '/tmp/foo.txt'", null, "/tmp/foo.txt", null));

    // unrelated file refused
    Assertions.assertFalse(ClientMessage.validateLocalFileName(sql, null, "/etc/passwd", null));
  }
}
