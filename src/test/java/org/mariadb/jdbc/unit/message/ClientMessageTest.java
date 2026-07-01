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

      public void encodeBinary(Writer encoder) {}

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
