// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;

/** plugin additional exchanges. raw data with mysql header packet */
public final class AuthMoreRawPacket implements ClientMessage {

  private final byte[] raw;

  /**
   * Constructor
   *
   * @param raw plugin exchange raw data
   */
  public AuthMoreRawPacket(byte[] raw) {
    this.raw = raw;
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {
    if (raw.length == 0) {
      writer.writeEmptyPacket();
    } else {
      writer.writeBytes(raw);
      writer.flush();
    }
    return 0;
  }
}
