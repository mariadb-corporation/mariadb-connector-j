// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.message;

import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.Parameter;
import org.mariadb.jdbc.client.util.Parameters;
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

  @Test
  public void validateLocalFileNameIgnoresQuotedText() {
    // a server controlled identifier (database / table name returned by the server and re-used in
    // a later query) must never satisfy a local file request, even when it contains the exact
    // text of a LOAD DATA LOCAL INFILE statement
    String payload = "LOAD DATA LOCAL INFILE '/etc/passwd'";
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW CREATE TABLE `" + payload + "`.`accounts`", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW TABLES FROM `" + payload + "`", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "UPDATE `" + payload + "`.`accounts` SET `c1` = ?  WHERE `id` = ? ",
            null,
            "/etc/passwd",
            null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT `id`,`c1` FROM `" + payload + "`.`accounts` WHERE `id` = ? ",
            null,
            "/etc/passwd",
            null));

    // identifier containing a statement separator, doubled backticks, or line breaks
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW CREATE TABLE `x; " + payload + "`.`t`", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW CREATE TABLE `x``; " + payload + "`.`t`", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW CREATE TABLE `x;\n" + payload + "`.`t`", null, "/etc/passwd", null));

    // payload inside string literals and comments
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT '; " + payload + "'", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT 'it''s; " + payload + "'", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT 'a\\'; " + payload + "'", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT \"; " + payload + "\"", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT 1 /* ; " + payload + " */", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT 1 -- ; " + payload + "\n", null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SELECT 1 # ; " + payload + "\n", null, "/etc/passwd", null));

    // payload not at a statement start
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName("SELECT 1 " + payload, null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName("SELECT 1, " + payload, null, "/etc/passwd", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName("SELECT 1 ? " + payload, null, "/etc/passwd", null));
  }

  @Test
  public void validateLocalFileNameStatementStart() {
    String load = "LOAD DATA LOCAL INFILE '/tmp/foo.txt' INTO TABLE t";

    // legitimate forms
    Assertions.assertTrue(ClientMessage.validateLocalFileName(load, null, "/tmp/foo.txt", null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName("  \n" + load, null, "/tmp/foo.txt", null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName("/* comment */ " + load, null, "/tmp/foo.txt", null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "-- comment\n# other\n" + load, null, "/tmp/foo.txt", null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOW_PRIORITY LOCAL INFILE '/tmp/foo.txt' INTO TABLE t",
            null,
            "/tmp/foo.txt",
            null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "LOAD XML CONCURRENT LOCAL INFILE '/tmp/foo.txt' INTO TABLE t",
            null,
            "/tmp/foo.txt",
            null));
    // multi-statement, LOAD DATA not being the first statement (CONJ-1164)
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "SET @a = 'x;y'; " + load + "; SELECT 1", null, "/tmp/foo.txt", null));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE '/tmp/other.txt' INTO TABLE t; " + load,
            null,
            "/tmp/foo.txt",
            null));
    // windows path : backslashes are doubled in the SQL literal
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE 'C:\\\\tmp\\\\foo.txt' INTO TABLE t",
            null,
            "C:\\tmp\\foo.txt",
            null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE 'C:\\\\tmp\\\\foo.txt' INTO TABLE t",
            null,
            "C:\\tmp\\bar.txt",
            null));

    // other file / unterminated or empty literal refused
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(load, null, "/tmp/foo.txt.bak", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE '/tmp/foo.txt", null, "/tmp/foo.txt", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName("LOAD DATA LOCAL INFILE ''", null, "", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName("SELECT 1", null, "/etc/passwd", null));
    Assertions.assertFalse(ClientMessage.validateLocalFileName(null, null, "/etc/passwd", null));
  }

  @Test
  public void validateLocalFileNameParameter() {
    Parameters params = singleParam(stringParameter("'/tmp/foo.txt'"));
    Assertions.assertTrue(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE ? INTO TABLE t", params, "/tmp/foo.txt", null));
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE ? INTO TABLE t", params, "/etc/passwd", null));
    // placeholder index is counted over previous statements
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "INSERT INTO t VALUES (?); LOAD DATA LOCAL INFILE ? INTO TABLE t",
            params,
            "/tmp/foo.txt",
            null));
    // no parameter at all
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "LOAD DATA LOCAL INFILE ? INTO TABLE t", null, "/tmp/foo.txt", null));
    // '?' inside a server controlled identifier is not a placeholder
    Assertions.assertFalse(
        ClientMessage.validateLocalFileName(
            "SHOW CREATE TABLE `LOAD DATA LOCAL INFILE ?`.`t`", params, "/tmp/foo.txt", null));
  }

  private static Parameter stringParameter(String value) {
    return new Parameter() {
      public void encodeText(Writer encoder, Context context) throws IOException {}

      public int getApproximateTextProtocolLength() {
        return -1;
      }

      public void encodeBinary(Writer encoder, Context context) {}

      public void encodeLongData(Writer encoder) {}

      public byte[] encodeData() {
        return new byte[0];
      }

      public boolean canEncodeLongData() {
        return false;
      }

      public int getBinaryEncodeType() {
        return 0;
      }

      public boolean isNull() {
        return false;
      }

      public String bestEffortStringValue(Context context) {
        return value;
      }
    };
  }

  @Test
  public void validateLocalFileNameUnknownParamRefused() {
    // parameterized LOAD DATA LOCAL INFILE where the bound parameter can't be rendered to a
    // literal (bestEffortStringValue returns null, e.g. a streaming parameter). The requested
    // file can't be matched against the query, so the server request must be refused.
    String sql = "LOAD DATA LOCAL INFILE ?";
    Parameters params = singleParam(unrenderableParameter());
    Assertions.assertFalse(ClientMessage.validateLocalFileName(sql, params, "/etc/passwd", null));
  }

  private static Parameter unrenderableParameter() {
    return new Parameter() {
      public void encodeText(Writer encoder, Context context) throws IOException {}

      public int getApproximateTextProtocolLength() {
        return -1;
      }

      public void encodeBinary(Writer encoder, Context context) {}

      public void encodeLongData(Writer encoder) {}

      public byte[] encodeData() {
        return new byte[0];
      }

      public boolean canEncodeLongData() {
        return true;
      }

      public int getBinaryEncodeType() {
        return 0;
      }

      public boolean isNull() {
        return false;
      }

      public String bestEffortStringValue(Context context) {
        return null;
      }
    };
  }

  private static Parameters singleParam(Parameter parameter) {
    return new Parameters() {
      public Parameter get(int index) {
        return parameter;
      }

      public boolean containsKey(int index) {
        return index == 0;
      }

      public void set(int index, Parameter element) {}

      public int size() {
        return 1;
      }

      public Parameters clone() {
        return this;
      }
    };
  }
}
