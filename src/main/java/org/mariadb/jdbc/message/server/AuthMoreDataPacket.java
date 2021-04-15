// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;

public class AuthMoreDataPacket implements ServerMessage {

  private final byte[] data;

  private AuthMoreDataPacket(byte[] data) {
    this.data = data;
  }

  public static AuthMoreDataPacket decode(ReadableByteBuf buf, Context context) {
    buf.skip(1);
    byte[] data = new byte[buf.readableBytes()];
    buf.readBytes(data);
    return new AuthMoreDataPacket(data);
  }

  public byte[] getData() {
    return data;
  }
}
